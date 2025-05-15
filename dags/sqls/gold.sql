-- Data Definition Language

-- CREATE SCHEMA IF NOT EXISTS PROFILES_VAULT.gold;

-- Create GOLD table
-- CREATE OR REPLACE TABLE PROFILES_VAULT.GOLD.customer_details(
--     customer_tracker INT,
--     id STRING,
--     first_name STRING,
--     last_name STRING,
--     gender STRING,
--     country STRING,
--     address STRING,
--     post_code STRING,
--     latitude FLOAT,
--     longitude FLOAT,
--     timezone STRING,
--     email STRING,
--     username STRING,
--     id_name STRING,
--     id_value STRING,
--     dob STRING,
--     age INT,
--     registered_date STRING,
--     phone STRING,
--     picture STRING,
--     nationality STRING
-- );

-- Data Manipulation Language
CREATE OR REPLACE PROCEDURE PROFILES_VAULT.GOLD.gl_load_customer_details()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    last_tracker INT;
BEGIN
    SELECT COALESCE(MAX(customer_tracker), 0)
    INTO :last_tracker
    FROM PROFILES_VAULT.GOLD.customer_details;

    INSERT INTO PROFILES_VAULT.GOLD.customer_details
    SELECT
        ROW_NUMBER() OVER(ORDER BY id) + :last_tracker AS customer_tracker,
        id,
        first_name,
        last_name,
        gender,
        country,
        address,
        post_code,
        latitude,
        longitude,
        timezone,
        email,
        username,
        id_name,
        id_value,
        dob,
        age,
        registered_date,
        phone,
        picture,
        nationality
    FROM PROFILES_VAULT.SILVER.customer_details
    WHERE id NOT IN (
        SELECT id FROM PROFILES_VAULT.GOLD.customer_details
    ) ORDER BY customer_tracker;

    COMMIT;
    RETURN 'GOLD Layer Insert completed successfully.';
END;
$$;

CALL PROFILES_VAULT.GOLD.gl_load_customer_details();
