    copy into OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT_v5 
  from @OUR_FIRST_DB.stage.LOAN_PAYMENT_stage_v1 
  pattern = '.*Loan.*.csv' 
  force= true ;
  



  ---orders -- 200/500
--

CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT_v6 (
  Loan_ID STRING,
  loan_status STRING,
  Principal STRING,
  terms STRING,
  effective_date STRING,
  due_date STRING,
  paid_off_time STRING,
  past_due_days STRING,
  age STRING,
  education varchar(3),
  Gender STRING);


     copy into OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT_v6
  from @OUR_FIRST_DB.stage.LOAN_PAYMENT_stage_v1 
  pattern = '.*Loan.*.csv' 
  TRUNCATECOLUMNS = TRUE ;


  select * from OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT_v6 ;



  ------------------ semi structured data 

    select OBJECT_CONSTRUCT(*) from OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT_v6 ;


SELECT PARSE_xml(loan_id , loan_status) from OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT_v6 ;


SELECT PARSE_XML(
  '<row><loan_id>' || loan_id || '</loan_id><loan_status>' || loan_status || '</loan_status></row>'
) FROM OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT_v6;



----json data into snowflake 

create database MANAGE_DB;

create schema EXTERNAL_STAGES;


--stage 
CREATE OR REPLACE stage MANAGE_DB.EXTERNAL_STAGES.JSONSTAGE
     url='s3://bucketsnowflake-jsondemo';
     

     list @MANAGE_DB.EXTERNAL_STAGES.JSONSTAGE ;


   --create table

   create table hr_data (raw_data  variant) ;


   copy into hr_data  
      from @MANAGE_DB.EXTERNAL_STAGES.JSONSTAGE 
      FILE_FORMAT = (type=json);


select * from hr_data ;
    

select $1:city:: string as city  ,
        $1:first_name,
        raw_data: gender,
        $1:job.salary as salary ,
        $1:job.title as titile ,
        $1:spoken_languages[0].language 
 from hr_data ;



 select $1:city:: string as city  ,
        $1:first_name,
        raw_data: gender
 from hr_data ;

SELECT
  hr.value:language::STRING AS language,
  hr.value:level::STRING AS level
FROM hr_data  ,
LATERAL FLATTEN(input => $1:spoken_languages) hr;



SELECT
  raw_data:id:: int as id ,
  hr.value:language::STRING AS language,
  hr.value:level::STRING AS level
FROM hr_data ,
LATERAL FLATTEN(input => raw_data:spoken_languages) hr;

  
--   {
--   "city": "Bakersfield",
--   "first_name": "Portia",
--   "gender": "Male",
--   "id": 1,
--   "job": {
--     "salary": 32000,
--     "title": "Financial Analyst"
--   },
--   "last_name": "Gioani",
--   "prev_company": [],
--   "spoken_languages": [
--     {
--       "language": "Kazakh",
--       "level": "Advanced"
--     },
--     {
--       "language": "Lao",
--       "level": "Basic"
--     }
--   ]
-- }
 


----Task 

1) load - parquet data and convert into a table format 
  url = 's3://snowflakeparquetdemo'  

2) URL =    's3://bucketsnowflakes3'   without creating the tables manually need to load data 
     hint: infer_schema 
3) Create 1 aws free tier for tomorrow class 
    https://www.youtube.com/watch?v=CCaVgwVGlt8&list=PLFPnaYaZXRwxdm2HQ4xM_wwwfYMRk0tcf
   create sample bucket 



1) creating stage 
2) list stage
3) file format 
4) Create tables 
5) Copy command to load data 






