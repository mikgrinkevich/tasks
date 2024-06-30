DROP TABLE IF EXISTS d_date;

CREATE TABLE d_date (
    date_dim_id int NOT NULL,
    date_actual date NOT NULL,
    epoch bigint NOT NULL,
    day_suffix varchar(4) NOT NULL,
    day_name varchar(9) NOT NULL,
    day_of_week int NOT NULL,
    day_of_month int NOT NULL,
    day_of_quarter int NOT NULL,
    day_of_year int NOT NULL,
    week_of_month int NOT NULL,
    week_of_year int NOT NULL,
    week_of_year_iso char(10) NOT NULL,
    month_actual int NOT NULL,
    month_name varchar(9) NOT NULL,
    month_name_abbreviated char(3) NOT NULL,
    quarter_actual int NOT NULL,
    quarter_name varchar(9) NOT NULL,
    year_actual int NOT NULL,
    first_day_of_week date NOT NULL,
    last_day_of_week date NOT NULL,
    first_day_of_month date NOT NULL,
    last_day_of_month date NOT NULL,
    first_day_of_quarter date NOT NULL,
    last_day_of_quarter date NOT NULL,
    first_day_of_year date NOT NULL,
    last_day_of_year date NOT NULL,
    mmyyyy char(6) NOT NULL,
    mmddyyyy char(10) NOT NULL,
    weekend_indr boolean NOT NULL
);

ALTER TABLE public.d_date
    ADD CONSTRAINT d_date_date_dim_id_pk PRIMARY KEY (date_dim_id);

CREATE INDEX d_date_date_actual_idx ON d_date (date_actual);

COMMIT;

INSERT INTO d_date
SELECT
    TO_CHAR(datum, 'yyyymmdd')::int AS date_dim_id,
    datum AS date_actual,
    EXTRACT(EPOCH FROM datum) AS epoch,
    TO_CHAR(datum, 'fmDDth') AS day_suffix,
    TO_CHAR(datum, 'TMDay') AS day_name,
    EXTRACT(ISODOW FROM datum) AS day_of_week,
    EXTRACT(DAY FROM datum) AS day_of_month,
    datum - DATE_TRUNC('quarter', datum)::date + 1 AS day_of_quarter,
    EXTRACT(DOY FROM datum) AS day_of_year,
    TO_CHAR(datum, 'W')::int AS week_of_month,
    EXTRACT(WEEK FROM datum) AS week_of_year,
    EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
    EXTRACT(MONTH FROM datum) AS month_actual,
    TO_CHAR(datum, 'TMMonth') AS month_name,
    TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
    EXTRACT(QUARTER FROM datum) AS quarter_actual,
    CASE WHEN EXTRACT(QUARTER FROM datum) = 1 THEN
        'First'
    WHEN EXTRACT(QUARTER FROM datum) = 2 THEN
        'Second'
    WHEN EXTRACT(QUARTER FROM datum) = 3 THEN
        'Third'
    WHEN EXTRACT(QUARTER FROM datum) = 4 THEN
        'Fourth'
    END AS quarter_name,
    EXTRACT(YEAR FROM datum) AS year_actual,
    datum + (1 - EXTRACT(ISODOW FROM datum))::int AS first_day_of_week,
    datum + (7 - EXTRACT(ISODOW FROM datum))::int AS last_day_of_week,
    datum + (1 - EXTRACT(DAY FROM datum))::int AS first_day_of_month,
    (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::date AS last_day_of_month,
    DATE_TRUNC('quarter', datum)::date AS first_day_of_quarter,
    (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::date AS last_day_of_quarter,
    TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
    TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
    TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
    TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN
        TRUE
    ELSE
        FALSE
    END AS weekend_indr
FROM (
    SELECT
        '1970-01-01'::date + SEQUENCE.DAY AS datum
    FROM
        GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
    GROUP BY
        SEQUENCE.DAY) DQ
ORDER BY
    1;

COMMIT;

