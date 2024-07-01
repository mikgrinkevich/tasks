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
  exit 1
fi

# Create the station_errors
psql postgresql://airflow:airflow@postgres/airflow <<EOF
CREATE TABLE IF NOT EXISTS station_errors (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    station VARCHAR(50) NOT NULL,
    msg VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL
);
EOF

# Truncate the station_errors 
psql postgresql://airflow:airflow@postgres/airflow <<EOF
TRUNCATE TABLE station_errors;
EOF

if [ $? -eq 0 ]; then
  echo "Table station_errors truncated successfully"
else
  echo "Failed to truncate table station_errors"
  exit 1
fi

# Process data and insert into station_errors
psql postgresql://airflow:airflow@postgres/airflow <<EOF
WITH filtered_data AS (
    SELECT
        id,
        date,
        station,
        msg
    FROM
        task_data
    WHERE
        msg != 'not finished'
),
ranked_data AS (
    SELECT
        id,
        date,
        station,
        msg,
        ROW_NUMBER() OVER (PARTITION BY station ORDER BY date) AS row_num,
        LAG(date) OVER (PARTITION BY station ORDER BY date) AS prev_date,
        LAG(date, 2) OVER (PARTITION BY station ORDER BY date) AS prev_prev_date
    FROM
        filtered_data
),
status_data AS (
    SELECT
        id,
        date,
        station,
        msg,
        prev_date,
        prev_prev_date,
        CASE
            WHEN prev_date IS NULL OR date != prev_date + INTERVAL '1 day' THEN 'new'
            ELSE 'unknown' -- Placeholder for further logic
        END AS initial_status
    FROM
        ranked_data
),
final_status_data AS (
    SELECT
        id,
        date,
        station,
        msg,
        CASE
            WHEN initial_status = 'new' THEN
                CASE
                    WHEN (SELECT COUNT(*) FROM filtered_data fd WHERE fd.date = sd.date AND fd.station = sd.station) > 1 THEN 'new'
                    ELSE 'new'
                END
            WHEN initial_status = 'unknown' THEN
                CASE
                    WHEN date = prev_date + INTERVAL '1 day' AND prev_date = prev_prev_date + INTERVAL '1 day' THEN 'critical'
                    WHEN date = prev_date + INTERVAL '1 day' THEN 'serious'
                    ELSE 'new'
                END
            ELSE initial_status
        END AS status
    FROM
        status_data sd
)
INSERT INTO station_errors (id, date, station, msg, status)
SELECT
    id,
    date,
    station,
    msg,
    status
FROM
    final_status_data
ORDER BY
    station,
    date;
EOF

# Log data processing
if [ $? -eq 0 ]; then
  echo "Data processed and inserted into station_errors successfully"
else
  echo "Failed to process data"
  exit 1
fi