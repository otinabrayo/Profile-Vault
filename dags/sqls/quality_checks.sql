CREATE OR REPLACE NOTIFICATION INTEGRATION my_s3_sns_integration
TYPE = QUEUE
ENABLED = TRUE
NOTIFICATION_PROVIDER = AWS_SNS
AWS_SNS_TOPIC_ARN = 'arn:aws:sns:eu-north-1:844075064641:profiles_vault_pipe'
AWS_SNS_ROLE_ARN = 'arn:aws:iam::844075064641:role/My_Snowflake_Role'
AWS_EXTERNAL_ID = 'UH41747_SFCRole=4_a2WopR7iTrBL42q1qAI2+S//tCs='
COMMENT = 'sns integration for profiles vault pipe';

aws sns subscribe \
  --topic-arn arn:aws:sns:eu-north-1:844075064641:profiles_vault_pipe \
  --protocol sqs \
  --notification-endpoint arn:aws:sqs:eu-north-1:844075064641:Coin_gecko_sqs_pipe

-- CREATE OR REPLACE PIPE PROFILES_VAULT.BRONZE.profiles_pipe
-- AUTO_INGEST = TRUE
-- AS
-- COPY INTO PROFILES_VAULT.BRONZE.customer_details_json_stage
-- FROM @MY_S3_STAGE/customer_details/
-- FILE_FORMAT = my_json_format;