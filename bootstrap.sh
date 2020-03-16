#!/bin/zsh
mkdir -p data/db/mongo
mkdir -p data/db/postgres

docker-compose up -d

# wait and retry till the postgres container is up
attempts=10
until docker exec postgres_benchmark bash -c pg_isready -h postgres_benchmark -p 5432; do
  if [ $attempts -lt 1 ]; then
    echo "Could not connect to postgres."
    exit 1
  fi
  echo "Failed to connect to postgres, waiting..."
  sleep 5
  ((attempts--))
done

# wait for database to start accepting connections
sleep 1

db_name="benchmark"

echo "Checking for PG database '$db_name'..."
does_table_exist="$(docker exec postgres_benchmark psql -U postgres -tc "select count(1) from pg_database where datname = '$db_name'")"

if $(echo "$does_table_exist" | grep -q 1); then
    echo "Database '$db_name' already exists!"
else
    echo "Creating PG database '$db_name'..."
    docker exec postgres_benchmark psql -U postgres -c "create database $db_name"
fi

echo 'Done!'
