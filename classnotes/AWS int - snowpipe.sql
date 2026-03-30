

-- Step 1: Create a storage integration to securely connect Snowflake with AWS S3
CREATE STORAGE INTEGRATION s3_int_vitech
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::931944281201:role/s3-vitech-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://vitech-netflix-test/csv/', 's3://vitech-netflix-test/json/')
  

-- Step 2: Describe the integration to get AWS_IAM_USER_ARN and AWS_EXTERNAL_ID for trust policy setup
describe integration s3_int_vitech;


-- Step 3: Create a CSV file format with comma delimiter, header skip, and double-quote enclosure
CREATE OR REPLACE FILE FORMAT OUR_FIRST_DB.PUBLIC.csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- Step 4: Create an external stage pointing to the S3 bucket using the storage integration
CREATE OR REPLACE STAGE OUR_FIRST_DB.PUBLIC.vitech_netflix_stage
    URL = 's3://vitech-netflix-test/csv/'
    STORAGE_INTEGRATION = s3_int_vitech
    FILE_FORMAT = OUR_FIRST_DB.PUBLIC.csv_format;

-- Step 5: List files in the stage to verify connectivity and see available files
    list @OUR_FIRST_DB.PUBLIC.vitech_netflix_stage;

-- Step 6: Create the target table to store Netflix data from S3
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.netflix_data (
    show_id       STRING,
    type          STRING,
    title         STRING,
    director      STRING,
    cast          STRING,
    country       STRING,
    date_added    STRING,
    release_year  INT,
    rating        STRING,
    duration      STRING,
    listed_in     STRING,
    description   STRING
);

-- Step 7: Manually load data from the stage into the table (one-time bulk load)
COPY INTO OUR_FIRST_DB.PUBLIC.netflix_data
FROM @OUR_FIRST_DB.PUBLIC.vitech_netflix_stage
FILE_FORMAT = OUR_FIRST_DB.PUBLIC.csv_format
ON_ERROR = 'CONTINUE';

-- Step 8: Query the table to verify data was loaded successfully
select * from OUR_FIRST_DB.PUBLIC.netflix_data;

------------------------------------------------------snow pipe -----
-- Step 9: Create a dedicated schema for Snowpipe objects
create schema  OUR_FIRST_DB.snowpipe  ;

-- Step 10: Create a Snowpipe with AUTO_INGEST to automatically load new files from S3 via SQS notifications
CREATE OR REPLACE PIPE OUR_FIRST_DB.snowpipe.netflix_pipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO OUR_FIRST_DB.PUBLIC.netflix_data
    FROM @OUR_FIRST_DB.PUBLIC.vitech_netflix_stage
    FILE_FORMAT = OUR_FIRST_DB.PUBLIC.csv_format
    ON_ERROR = 'CONTINUE';

-- Step 11: Describe the pipe to get the notification_channel (SQS ARN) for S3 event setup
describe pipe OUR_FIRST_DB.snowpipe.netflix_pipe;


-- Step 12: Manually refresh the pipe to pick up any existing files in the stage
alter pipe OUR_FIRST_DB.snowpipe.netflix_pipe refresh ;

-- Step 13: Verify data loaded by Snowpipe
select * from OUR_FIRST_DB.PUBLIC.netflix_data;

-- Step 14: Check the current status of the pipe (pending files, execution state, last ingested timestamp)
SELECT SYSTEM$PIPE_STATUS('OUR_FIRST_DB.SNOWPIPE.NETFLIX_PIPE');

-- Step 15: Pause the pipe to stop auto-ingestion
ALTER PIPE OUR_FIRST_DB.SNOWPIPE.NETFLIX_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;

-- Step 16: Verify pipe is paused
SELECT SYSTEM$PIPE_STATUS('OUR_FIRST_DB.SNOWPIPE.NETFLIX_PIPE');

-- Step 17: Resume the pipe to restart auto-ingestion
ALTER PIPE OUR_FIRST_DB.SNOWPIPE.NETFLIX_PIPE SET PIPE_EXECUTION_PAUSED = FALSE;

-- Step 18: Verify pipe is running again
SELECT SYSTEM$PIPE_STATUS('OUR_FIRST_DB.SNOWPIPE.NETFLIX_PIPE');

---Assignment 
---- cretae aws integartion and  load json data give me the count 
--- implemnet snowpipe for json data 
-- implement snow pipe for employee csv data 
--- load employee-1 csv data into snowflake ... modify the same file add 10 rows and upload into s3 
   -- try to load data and observer ???  10 --> inserted or not or toal ??
   --delete 50 records from csv file and load the same file into s3 and observe 

   create  table employee ( id int , address string);

