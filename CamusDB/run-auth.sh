export CAMUSDB_AUTH_ENABLED=true
export CAMUSDB_AUTH_TOKEN_KEY="$(openssl rand -hex 32)"
export CAMUSDB_BOOTSTRAP_USER=admin
export CAMUSDB_BOOTSTRAP_PASSWORD="$(openssl rand -base64 24)"
export CAMUSDB_NODE_SECRET="$(openssl rand -hex 32)"
echo $CAMUSDB_BOOTSTRAP_PASSWORD
dotnet run
