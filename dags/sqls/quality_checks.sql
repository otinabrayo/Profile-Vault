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


CREATE OR REPLACE STORAGE INTEGRATION s3_storage_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::844075064641:role/My_Snowflake_Role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://data.engineering.projects/');


SELECT CURRENT_ROLE();
USE ROLE ACCOUNTADMIN;

-- DROP INTEGRATION s3_storage_integration;

DESCRIBE INTEGRATION s3_storage_integration;

SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION(
  'S3_STORAGE_INTEGRATION',
  's3://data.engineering.projects/customer_details/',
  'organiclion754.json',
  'LIST'
);

CREATE OR REPLACE STAGE MY_S3_STAGE
  URL = 's3://data.engineering.projects/'
  STORAGE_INTEGRATION = s3_storage_integration;

  SHOW GRANTS ON INTEGRATION s3_storage_integration;


SHOW STORAGE INTEGRATIONS;
GRANT USAGE ON INTEGRATION s3_storage_integration TO ROLE SECURITYADMIN;

LIST @mystage_2nd_stage;

CREATE OR REPLACE PIPE PROFILES_VAULT.BRONZE.profiles_pipe
AUTO_INGEST = TRUE
AS
COPY INTO PROFILES_VAULT.BRONZE.customer_details_json_stage
FROM @MY_S3_STAGE/customer_details/
FILE_FORMAT = my_json_format
ON_ERROR = 'CONTINUE';


SHOW PIPES;
SELECT SYSTEM$PIPE_STATUS('profiles_pipe');


CREATE OR REPLACE NOTIFICATION INTEGRATION my_s3_sns_integration
TYPE = QUEUE
ENABLED = TRUE
NOTIFICATION_PROVIDER = AWS_SNS
DIRECTION = OUTBOUND
AWS_SNS_TOPIC_ARN = 'arn:aws:sns:eu-north-1:844075064641:profiles_vault_pipe'
AWS_SNS_ROLE_ARN = 'arn:aws:iam::844075064641:role/My_Snowflake_Role';

DESC NOTIFICATION INTEGRATION my_s3_sns_integration;

TRUNCATE PROFILES_VAULT.BRONZE.customer_details_json_stage;

SELECT * FROM PROFILES_VAULT.BRONZE.customer_details_json_stage;

select system$get_aws_sns_iam_policy('arn:aws:sns:eu-north-1:844075064641:profiles_vault_pipe');

select system$get_aws_sns_iam_policy('arn:aws:sns:eu-north-1:844075064641:profiles_topic');

LIST @my_s3_stage/customer_details/;
ALTER PIPE profiles_pipe REFRESH;

SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  table_name => 'customer_details_json_stage',
  start_time => DATEADD(hour, -1, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;

SELECT * FROM PROFILES_VAULT.SILVER.CUSTOMER_DETAILS;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE PIPE_NAME = 'profiles_pipe';

COPY INTO PROFILES_VAULT.BRONZE.customer_details_json_stage
FROM @PROFILES_VAULT.BRONZE.my_s3_stage/customer_details/
FILE_FORMAT = (TYPE = 'JSON');

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
FROM PROFILES_VAULT.BRONZE.customer_details
WHERE SUBSTR(id, 1, 5) NOT IN (
SELECT id FROM PROFILES_VAULT.silver.customer_details
);


SELECT
  ROW_NUMBER() OVER(ORDER BY id) as customer_tracker,
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

SELECT COUNT(*)
FROM PROFILES_VAULT.BRONZE.customer_details_json_stage
WHERE raw:id::STRING NOT IN (
SELECT id FROM PROFILES_VAULT.BRONZE.customer_details
);

SELECT SUBSTR(id, 1, 5) AS id FROM PROFILES_VAULT.BRONZE.customer_details;

SELECT * FROM PROFILES_VAULT.GOLD.customer_details;

DESCRIBE STAGE my_s3_stage;

CREATE OR REPLACE FILE FORMAT my_json_format
  TYPE = 'JSON'
  ENCODING = 'UTF8';


TRUNCATE PROFILES_VAULT.GOLD.customer_details;
SELECT DISTINCT ID_VALUE FROM PROFILES_VAULT.SILVER.customer_details;
