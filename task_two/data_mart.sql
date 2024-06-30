CREATE SCHEMA data_mart;

CREATE MATERIALIZED VIEW data_mart.deposits_withdrawals AS
WITH deposits AS (
    SELECT
        sum(amount) AS total_deposits_amount,
        date(created_at) AS date
    FROM
        deposits
    GROUP BY
        date(created_at)
),
withdrawals AS (
    SELECT
        sum(amount) AS total_withdrawals_amount,
        date(created_at) AS date
    FROM
        withdrawals
    GROUP BY
        date(created_at))
SELECT
    d.total_deposits_amount,
    w.total_withdrawals_amount,
    (d.total_deposits_amount - w.total_withdrawals_amount) AS variance,
    d.date
FROM
    deposits d
    JOIN withdrawals w ON w.date = d.date
    JOIN d_date ON d_date.date_actual = d.date CREATE EXTENSION pg_cron;

CREATE EXTENSION pg_cron;

SELECT
    cron.schedule ('0 0 * * *', 'refresh materialized view data_mart.deposits_withdrawals;');

