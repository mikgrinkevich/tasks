#!/bin/bash

DB_NAME="airflow"
DB_USER="airflow"
DB_PASSWORD="airflow"
DB_HOST="postgres"
DB_PORT="5432"

CSV_FILE="/opt/airflow/scripts/events.csv"
OUTPUT_CSV="/opt/airflow/scripts/events_cleaned.csv"

psql postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_NAME <<EOF
DROP TABLE IF EXISTS events;
CREATE TABLE events (
    user_id VARCHAR(255) PRIMARY KEY,
    product_identifier VARCHAR(255),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    price_in_usd FLOAT
);
EOF

# Drop duplicated records
awk -F',' '!seen[$1]++' "$CSV_FILE" > "$OUTPUT_CSV"

psql postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_NAME <<EOF
\copy events(user_id, product_identifier, start_time, end_time, price_in_usd) FROM '$OUTPUT_CSV' DELIMITER ',' CSV HEADER;
EOF

echo "Table recreated, data inserted"
