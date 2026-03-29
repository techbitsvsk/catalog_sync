#!/usr/bin/env bash
set -e

# Creates: nessie/nessie, oauth/oauth, airflow/airflow databases
function create_user_and_db() {
    local db=$1
    echo "  Creating user and database '$db'"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        CREATE USER $db WITH PASSWORD '$db';
        CREATE DATABASE $db OWNER $db;
        GRANT ALL PRIVILEGES ON DATABASE $db TO $db;
EOSQL
}

for db in nessie oauth airflow; do
    create_user_and_db "$db"
done
echo "All databases initialized."
