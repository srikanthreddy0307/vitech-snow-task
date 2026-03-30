

-----------unload 

-- Step 1: Create a storage integration to securely connect Snowflake with AWS S3
CREATE STORAGE INTEGRATION s3_int_vitech1
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::632759406488:role/vitech_s3_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://vitech-netflix-test1/csv/', 's3://vitech-netflix-test1/json/')
  

-- Step 2: Describe the integration to get AWS_IAM_USER_ARN and AWS_EXTERNAL_ID for trust policy setup
describe integration s3_int_vitech1;


-- Step 3: Create a CSV file format with comma delimiter, header skip, and double-quote enclosure
CREATE OR REPLACE FILE FORMAT OUR_FIRST_DB.PUBLIC.csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- Step 4: Create an external stage pointing to the S3 bucket using the storage integration
CREATE OR REPLACE STAGE OUR_FIRST_DB.PUBLIC.vitech_netflix_stage
    URL = 's3://vitech-netflix-test1/csv/'
    STORAGE_INTEGRATION = s3_int_vitech1
    FILE_FORMAT = OUR_FIRST_DB.PUBLIC.csv_format;

-- Step 5: List files in the stage to verify connectivity and see available files
    list @OUR_FIRST_DB.PUBLIC.vitech_netflix_stage;


    select * from OUR_FIRST_DB.PUBLIC.netflix_data;

      select object_construct(*)  from OUR_FIRST_DB.PUBLIC.netflix_data;


-- Step 6: Create a JSON file format for unloading
CREATE OR REPLACE FILE FORMAT OUR_FIRST_DB.PUBLIC.json_format
    TYPE = 'JSON';

-- Step 7: Create an external stage pointing to the S3 JSON folder
CREATE OR REPLACE STAGE OUR_FIRST_DB.PUBLIC.vitech_netflix_json_stage
    URL = 's3://vitech-netflix-test1/json/'
    STORAGE_INTEGRATION = s3_int_vitech1
    FILE_FORMAT = OUR_FIRST_DB.PUBLIC.json_format;

-- Step 8: Unload netflix_data to S3 JSON folder
COPY INTO @OUR_FIRST_DB.PUBLIC.vitech_netflix_json_stage
FROM (
    SELECT OBJECT_CONSTRUCT(*) 
    FROM OUR_FIRST_DB.PUBLIC.netflix_data 
)
FILE_FORMAT = (TYPE = 'JSON')
OVERWRITE = TRUE;


-- Step 8: Unload netflix_data to S3 JSON folder
COPY INTO @OUR_FIRST_DB.PUBLIC.vitech_netflix_json_stage
FROM (
    SELECT * 
    FROM OUR_FIRST_DB.PUBLIC.netflix_data 
)
FILE_FORMAT = (TYPE = 'csv')
OVERWRITE = TRUE;

-- Step 9: Verify files were unloaded
LIST @OUR_FIRST_DB.PUBLIC.vitech_netflix_json_stage;

-------------------------------------------

     select * from OUR_FIRST_DB.PUBLIC.netflix_data;


  select * from OUR_FIRST_DB.PUBLIC.netflix_data1;
  
     update OUR_FIRST_DB.PUBLIC.netflix_data
        set director = 'Bhageeratha' ;

--offeset timestamp query id 

create table OUR_FIRST_DB.PUBLIC.netflix_data1 as (
    select * from OUR_FIRST_DB.PUBLIC.netflix_data at (offset => -60 * 2 )
);


show tables ;



ALTER TABLE OUR_FIRST_DB.PUBLIC.netflix_data SET DATA_RETENTION_TIME_IN_DAYS = 30;


delete from OUR_FIRST_DB.PUBLIC.netflix_data ;

select sysdate() ;

-- Time Travel using TIMESTAMP (replace with your desired timestamp)
SELECT * FROM OUR_FIRST_DB.PUBLIC.netflix_data
    AT(TIMESTAMP => '2026-03-27 04:46:01.673'::TIMESTAMP);

create or replace table OUR_FIRST_DB.PUBLIC.netflix_data as (
    SELECT * FROM OUR_FIRST_DB.PUBLIC.netflix_data
    AT(TIMESTAMP => '2026-03-27 04:46:01.673'::TIMESTAMP) ) ;

-- Time Travel using QUERY ID (replace with actual query ID from query history)
SELECT * FROM OUR_FIRST_DB.PUBLIC.netflix_data
    BEFORE(STATEMENT => '01c34d03-0001-95f3-000e-08f600036c02');



delete from OUR_FIRST_DB.PUBLIC.netflix_data  where type ='Movie' ;


    ----------------------
    

create schema hr.public_cpy 
        clone our_first_db.public ;


drop stage HR.PUBLIC_CPY.VITECH_NETFLIX_JSON_STAGE  ;


create  database OUR_FIRST_DB_dev2  clone OUR_FIRST_DB ; 


   