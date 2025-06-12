use mobc::{Manager, Pool, async_trait};
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
pub struct PostgresInfoStorage {
    pool: Pool<PgConnectionManager<NoTls>>,
}

impl PostgresInfoStorage {
    /// Create new `PostgresInfoStorage`.
    ///
    /// # Errors
    ///
    /// Might return an error, if postgres client cannot be created.
    pub fn new(db_dsn: &str, _expiration: Option<usize>) -> RustusResult<Self> {
        let config = db_dsn.parse::<Config>().map_err(|e| RustusError::UnableToPrepareInfoStorage(e.to_string()))?;
        let manager = PgConnectionManager::new(config, NoTls);
        let pool = mobc::Pool::builder().max_open(100).build(manager);
        Ok(Self { pool })
    }
}

impl InfoStorage for PostgresInfoStorage {
    async fn prepare(&mut self) -> RustusResult<()> {
        let create_table_query = r#"
        CREATE TABLE IF NOT EXISTS file_info (
            id TEXT PRIMARY KEY,
            info JSONB NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        )"#;

        let conn = self.pool.get().await?;
        conn.execute(create_table_query, &[]).await?;
        Ok(())
    }

    async fn set_info(&self, file_info: &FileInfo, create: bool) -> RustusResult<()> {
        let json_info = serde_json::to_string(file_info)?;
        let conn = self.pool.get().await?;

        if create {
            // Insert new record
            let query = r#"
            INSERT INTO file_info (id, info)
            VALUES ($1, $2)
            "#;
            
            conn.execute(
                query,
                &[
                    &file_info.id,
                    &json_info,
                ],
            ).await?;
        } else {
            // Update existing record
            let query = r#"
            UPDATE file_info
            SET info = $2
            WHERE id = $1
            "#;
            
            let result = conn.execute(
                query,
                &[
                    &file_info.id,
                    &json_info,
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
        
        let query = r#"
        SELECT info FROM file_info
        WHERE id = $1
        "#;
        
        let row = conn.query_opt(query, &[&file_id]).await?;
        
        match row {
            Some(row) => {
                let json_info: String = row.get(0);
                let file_info = serde_json::from_str(&json_info)?;
                Ok(file_info)
            }
            None => Err(RustusError::FileNotFound),
        }
    }

    async fn remove_info(&self, file_id: &str) -> RustusResult<()> {
        let conn = self.pool.get().await?;
        
        let query = r#"
        DELETE FROM file_info
        WHERE id = $1
        "#;
        
        let result = conn.execute(query, &[&file_id]).await?;
        
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

    fn get_url() -> String {
        std::env::var("TEST_POSTGRES_URL")
            .unwrap_or("postgres://postgres:postgres@localhost/rustus".to_string())
    }

    async fn get_storage() -> PostgresInfoStorage {
        let mut storage = PostgresInfoStorage::new(get_url().as_str(), None).unwrap();
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
        let info_storage = PostgresInfoStorage::new("postgres://unknown_user:password@unknown_host/db", None).unwrap();
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

