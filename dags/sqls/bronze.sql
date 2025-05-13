-- Data Definition Language

/*
USE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS profiles_vault;

CREATE SCHEMA IF NOT EXISTS profiles_vault.bronze;

Create file format
CREATE OR REPLACE FILE FORMAT PROFILES_VAULT.BRONZE.my_json_format TYPE = 'JSON';

Create  staging table  (files from external stage)
CREATE OR REPLACE TABLE PROFILES_VAULT.BRONZE.customer_details_json_stage (raw VARIANT);

Create stage in snowflake (externally)
CREATE OR REPLACE STAGE PROFILES_VAULT.BRONZE.MY_S3_STAGE
    URL = 's3://data.engineering.projects/'
    CREDENTIALS = (AWS_KEY_ID='your_key' AWS_SECRET_KEY='your_key')
    FILE_FORMAT = (TYPE = 'JSON');
*/

-- Create bronze table
CREATE OR REPLACE TABLE PROFILES_VAULT.BRONZE.customer_details(
    id STRING,
    first_name STRING,
    last_name STRING,
    gender STRING,
    country STRING,
    address STRING,
    post_code STRING,
    latitude STRING,
    longitude STRING,
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
CREATE OR REPLACE PROCEDURE PROFILES_VAULT.BRONZE.br_load_customer_details()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO PROFILES_VAULT.BRONZE.customer_details
    SELECT
        raw:id::STRING AS key_id,
        raw:first_name::STRING AS first_name,
        raw:last_name::STRING AS last_name,
        raw:gender::STRING AS gender,
        raw:country::STRING AS country,
        raw:address::STRING AS address,
        raw:post_code::STRING AS post_code,
        raw:latitude::STRING AS latitude,
        raw:longitude::STRING AS longitude,
        raw:timezone::STRING AS timezone,
        raw:email::STRING AS email,
        raw:username::STRING AS username,
        raw:id_name::STRING AS id_name,
        raw:id_value::STRING AS id_value,
        raw:dob::STRING AS dob,
        raw:age::INT AS age,
        raw:registered_date::STRING AS registered_date,
        raw:phone::STRING AS phone,
        raw:picture::STRING AS picture,
        raw:nationality::STRING AS nationality
    FROM PROFILES_VAULT.BRONZE.customer_details_json_stage;

    COMMIT;
    RETURN 'Bronze Layer Insert completed successfully.';
END;
$$;

CALL PROFILES_VAULT.BRONZE.br_load_customer_details();