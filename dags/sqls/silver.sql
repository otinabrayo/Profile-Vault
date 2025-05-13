-- Data Definition Language

-- CREATE SCHEMA IF NOT EXISTS PROFILES_VAULT.silver;

-- Create SILVER table
CREATE OR REPLACE TABLE PROFILES_VAULT.SILVER.customer_details(
    id STRING,
    first_name STRING,
    last_name STRING,
    gender STRING,
    country STRING,
    address STRING,
    post_code STRING,
    latitude FLOAT,
    longitude FLOAT,
    timezone STRING,
    email STRING,
    username STRING,
    id_name STRING,
    id_value STRING,
    dob STRING,
    age INT,
    registered_date STRING,
    phone STRING,
    picture STRING,
    nationality STRING
);

-- Data Manipulation Language
CREATE OR REPLACE PROCEDURE PROFILES_VAULT.SILVER.sl_load_customer_details()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO PROFILES_VAULT.SILVER.customer_details
    SELECT
        SUBSTR(id, 1, 5) AS id,
        first_name,
        last_name,
        CASE
            WHEN gender = 'male' THEN 'Male'
            WHEN gender = 'female' THEN 'Female'
            ELSE gender
        END AS gender,
        country,
        address,
        post_code,
        CAST(latitude AS FLOAT) AS latitude,
        CAST(longitude AS FLOAT) AS longitude,
        timezone,
        REPLACE(email, 'example.com', 'yahoo.com') AS email,
        username,
        CASE
            WHEN id_name = '' THEN null
            ELSE id_name
        END AS id_name,
        id_value,
        TO_CHAR(CAST(dob AS TIMESTAMP_TZ), 'YYYY-MM-DD HH24:MI')AS dob,
        age,
        TO_CHAR(CAST(registered_date AS TIMESTAMP_TZ), 'YYYY-MM-DD HH24:MI')AS registered_date,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) = 10 THEN '+1' || REGEXP_REPLACE(phone, '[^0-9]', '')
            ELSE REGEXP_REPLACE(phone, '[^0-9]', '')
        END AS phone_number,
        picture,
        nationality
    FROM PROFILES_VAULT.BRONZE.customer_details;

    COMMIT;
    RETURN 'SILVER Layer Insert completed successfully.';
END;
$$;

CALL PROFILES_VAULT.SILVER.sl_load_customer_details();