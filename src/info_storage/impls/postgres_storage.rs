use mobc::{Manager, Pool, async_trait};
use tokio_postgres::config::SslMode;
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect, NoTls};
use tokio_postgres::{Client, Config, Error, Socket};

use crate::{
    errors::{RustusError, RustusResult},
    file_info::FileInfo,
    info_storage::base::InfoStorage,
};

struct PgConnectionManager<Tls> {
    config: Config,
    tls: Tls,
}

impl<Tls> PgConnectionManager<Tls> {
    pub fn new(config: Config, tls: Tls) -> Self {
        Self { config, tls }
    }
}

#[async_trait]
impl<Tls> Manager for PgConnectionManager<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type Connection = Client;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let tls = self.tls.clone();
        let (client, conn) = self.config.connect(tls).await?;
        mobc::spawn(conn);
        Ok(client)
    }

    async fn check(&self, conn: Self::Connection) -> Result<Self::Connection, Self::Error> {
        conn.simple_query("").await?;
        Ok(conn)
    }
}

#[derive(Clone, Debug)]
pub struct PostgresInfoStorageConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub db_name: String,
    pub table_name: String,
    pub schema_name: String,
}

#[derive(Clone, Debug)]
pub struct PostgresInfoStorage {
    pool: Pool<PgConnectionManager<NoTls>>,
    table_name: String,
    schema_name: String,
}

impl PostgresInfoStorage {
    /// Create new `PostgresInfoStorage`.
    ///
    /// # Errors
    ///
    /// Might return an error, if postgres client cannot be created.
    pub fn new(config: &PostgresInfoStorageConfig) -> RustusResult<Self> {
        let mut new_pg_config = Config::new();
        let pg_config = new_pg_config
            .host(&config.host)
            .port(config.port)
            .user(&config.user)
            .password(&config.password)
            .dbname(&config.db_name)
            .ssl_mode(SslMode::Disable);
        let manager = PgConnectionManager::new(pg_config.clone(), NoTls);
        let pool = mobc::Pool::builder().max_open(100).build(manager);
        Ok(Self { pool, table_name: config.table_name.clone(), schema_name: config.schema_name.clone() })
    }
}

impl InfoStorage for PostgresInfoStorage {
    async fn prepare(&mut self) -> RustusResult<()> {
        let create_table_query = format!(r#"
        CREATE TABLE IF NOT EXISTS {}.{} (
            id TEXT PRIMARY KEY,
            "offset" BIGINT NOT NULL,
            length BIGINT,
            path TEXT,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL,
            deferred_size BOOLEAN NOT NULL,
            is_partial BOOLEAN NOT NULL,
            is_final BOOLEAN NOT NULL,
            parts TEXT[],
            storage TEXT NOT NULL,
            metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb
        )"#, self.schema_name, self.table_name);

        let conn = self.pool.get().await?;
        conn.execute(&create_table_query, &[]).await?;
        Ok(())
    }

    async fn set_info(&self, file_info: &FileInfo, create: bool) -> RustusResult<()> {
        let metadata_json = serde_json::to_value(&file_info.metadata)?;
        let conn = self.pool.get().await?;

        if create {
            // Insert new record
            let query = format!(r#"
            INSERT INTO {}.{} (id, "offset", length, path, created_at, deferred_size, is_partial, is_final, parts, storage, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#, self.schema_name, self.table_name);

            let length_param: Option<i64> = file_info.length.map(|l| l as i64);
            let parts_param: Option<Vec<String>> = file_info.parts.clone();

            conn.execute(
                &query,
                &[
                    &file_info.id,
                    &(file_info.offset as i64),
                    &length_param,
                    &file_info.path,
                    &file_info.created_at,
                    &file_info.deferred_size,
                    &file_info.is_partial,
                    &file_info.is_final,
                    &parts_param,
                    &file_info.storage,
                    &metadata_json,
                ],
            ).await?;
        } else {
            // Update existing record
            let query = format!(r#"
            UPDATE {}.{} SET 
                "offset" = $2,
                length = $3,
                path = $4,
                created_at = $5,
                deferred_size = $6,
                is_partial = $7,
                is_final = $8,
                parts = $9,
                storage = $10,
                metadata = $11
            WHERE id = $1
            "#, self.schema_name, self.table_name);

            let length_param: Option<i64> = file_info.length.map(|l| l as i64);
            let parts_param: Option<Vec<String>> = file_info.parts.clone();

            let result = conn.execute(
                &query,
                &[
                    &file_info.id,
                    &(file_info.offset as i64),
                    &length_param,
                    &file_info.path,
                    &file_info.created_at,
                    &file_info.deferred_size,
                    &file_info.is_partial,
                    &file_info.is_final,
                    &parts_param,
                    &file_info.storage,
                    &metadata_json,
                ],
            ).await?;

            if result == 0 {
                return Err(RustusError::FileNotFound);
            }
        }

        Ok(())
    }

    async fn get_info(&self, file_id: &str) -> RustusResult<FileInfo> {
        let conn = self.pool.get().await?;

        let query = format!(r#"
        SELECT id, "offset", length, path, created_at, deferred_size, is_partial, is_final, parts, storage, metadata 
        FROM {}.{}
        WHERE id = $1
        "#, self.schema_name, self.table_name);

        let row = conn.query_opt(&query, &[&file_id]).await?;
        
        match row {
            Some(row) => {
                let id: String = row.get(0);
                let offset: i64 = row.get(1);
                let length: Option<i64> = row.get(2);
                let path: Option<String> = row.get(3);
                let created_at: chrono::DateTime<chrono::Utc> = row.get(4);
                let deferred_size: bool = row.get(5);
                let is_partial: bool = row.get(6);
                let is_final: bool = row.get(7);
                let parts: Option<Vec<String>> = row.get(8);
                let storage: String = row.get(9);
                let metadata_json: serde_json::Value = row.get(10);
                
                let metadata: std::collections::HashMap<String, String> = serde_json::from_value(metadata_json)?;
                
                let file_info = FileInfo {
                    id,
                    offset: offset as usize,
                    length: length.map(|l| l as usize),
                    path,
                    created_at,
                    deferred_size,
                    is_partial,
                    is_final,
                    parts,
                    storage,
                    metadata,
                };
                
                Ok(file_info)
            }
            None => Err(RustusError::FileNotFound),
        }
    }

    async fn remove_info(&self, file_id: &str) -> RustusResult<()> {
        let conn = self.pool.get().await?;

        let query = format!(r#"
        DELETE FROM {}.{}
        WHERE id = $1
        "#, self.schema_name, self.table_name);

        let result = conn.execute(&query, &[&file_id]).await?;

        match result {
            0 => Err(RustusError::FileNotFound),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{file_info::FileInfo, info_storage::base::InfoStorage};
    use super::PostgresInfoStorage;
    use super::PostgresInfoStorageConfig;

    fn get_config() -> PostgresInfoStorageConfig {
        PostgresInfoStorageConfig {
            host: "localhost".into(),
            user: "postgres".into(),
            password: "postgres".into(),
            db_name: "rustus".into(),
            port: 5432,
            table_name: "file_info".into(),
            schema_name: "public".into(),
        }
    }

    async fn get_storage() -> PostgresInfoStorage {
        let mut storage = PostgresInfoStorage::new(&get_config()).unwrap();
        storage.prepare().await.unwrap();
        storage
    }

    #[actix_rt::test]
    async fn success() {
        let info_storage = get_storage().await;
        let file_info = FileInfo::new_test();
        
        // Create a new file info
        info_storage.set_info(&file_info, true).await.unwrap();
        
        // Retrieve the file info
        let file_info_from_storage = info_storage.get_info(file_info.id.as_str()).await.unwrap();
        
        // Assert equality
        assert_eq!(file_info.id, file_info_from_storage.id);
        assert_eq!(file_info.path, file_info_from_storage.path);
        assert_eq!(file_info.storage, file_info_from_storage.storage);
    }

    #[actix_rt::test]
    async fn no_connection() {
        let mut config = get_config();
        config.host = "invalid_host".into(); // Set an invalid host to simulate no connection
        let info_storage = PostgresInfoStorage::new(&config).unwrap();
        let file_info = FileInfo::new_test();
        
        let res = info_storage.set_info(&file_info, true).await;
        assert!(res.is_err());
    }

    #[actix_rt::test]
    async fn unknown_id() {
        let info_storage = get_storage().await;
        
        let res = info_storage
            .get_info(uuid::Uuid::new_v4().to_string().as_str())
            .await;
        
        assert!(res.is_err());
    }

    #[actix_rt::test]
    async fn deletion_success() {
        let info_storage = get_storage().await;
        let file_info = FileInfo::new_test();
        
        // Create a new file info
        info_storage.set_info(&file_info, true).await.unwrap();
        
        // Delete the file info
        info_storage.remove_info(&file_info.id).await.unwrap();
        
        // Try to get the deleted file info, should fail
        let res = info_storage.get_info(&file_info.id).await;
        assert!(res.is_err());
    }
}

