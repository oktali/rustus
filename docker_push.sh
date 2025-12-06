# !/bin/bash
read -p "Enter version number: " version

sudo docker build -t ghcr.io/oktali/rustus:$version .
sudo docker push ghcr.io/oktali/rustus:$version