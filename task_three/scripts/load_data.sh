#!/bin/bash

echo "Starting load_data.sh script"

while ! pg_isready -h postgres -p 5432 -U airflow; do
  echo "Waiting for PostgreSQL to start..."
  sleep 2
done

echo "PostgreSQL is ready"

psql postgresql://airflow:airflow@postgres/airflow <<EOF
CREATE TABLE IF NOT EXISTS task_data (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    station VARCHAR(50) NOT NULL,
    msg VARCHAR(50) NOT NULL
);
EOF

psql postgresql://airflow:airflow@postgres/airflow <<EOF
\copy task_data(id, date, station, msg) FROM '/opt/airflow/scripts/diagnostics.csv' DELIMITER ',' CSV HEADER;
EOF

# Log data loading
if [ $? -eq 0 ]; then
  echo "Data loaded successfully"
else
  echo "Failed to load data"
fi