

create database vitech_dev ;


create table vitech_dev.public.orders  clone 
                                SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS ;

create table vitech_dev.public.orders  as 
(select top 10000 * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS) ;

select * from vitech_dev.public.orders ;

select substr(o_orderpriority,0,1) from vitech_dev.public.orders ;

CREATE OR REPLACE FUNCTION VITECH_DEV.PUBLIC.GET_FIRST_CHAR(input_str STRING)
RETURNS STRING
AS
$$
    SUBSTR(input_str, 0, 1)
$$;

SELECT O_CUSTKEY,O_ORDERSTATUS,GET_FIRST_CHAR(O_CLERK), GET_FIRST_CHAR(o_orderpriority) FROM VITECH_DEV.PUBLIC.ORDERS;



CREATE OR REPLACE FUNCTION VITECH_DEV.PUBLIC.GET_sum(input_str1 INT,input_str2 INT)
RETURNS INT
AS
$$
   INPUT_STR1 + input_str2 
$$;


SELECT GET_sum(30,20) ;

SELECT SYSDATE() ;

CREATE OR REPLACE TABLE VITECH_DEV.PUBLIC.EMPLOYEE (
    EID INT,
    NAME VARCHAR(100),
    SALARY NUMBER(12,2),
    DEPARTMENT VARCHAR(50),
    STATUS VARCHAR(20)
);

INSERT INTO VITECH_DEV.PUBLIC.EMPLOYEE (EID, NAME, SALARY, DEPARTMENT,STATUS ) VALUES
    (1, 'Alice Johnson', 75000.00, 'Engineering','aCTIVE') ;
  

SELECT * FROM EMPLOYEE;

CREATE OR REPLACE PROCEDURE VITECH_DEV.PUBLIC.INSERT_EMPLOYEE(
    P_EID INT,
    P_NAME STRING,
    P_SALARY NUMBER(12,2),
    P_DEPARTMENT VARCHAR(30),
    P_STATUS VARCHAR(30)
)
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    INSERT INTO VITECH_DEV.PUBLIC.EMPLOYEE (EID, NAME, SALARY, DEPARTMENT, STATUS)
        VALUES (:P_EID, :P_NAME, :P_SALARY, :P_DEPARTMENT, :P_STATUS);
    RETURN 'Row inserted successfully';
END;


----------------
SELECT * FROM EMPLOYEE;

CALL VITECH_DEV.PUBLIC.INSERT_EMPLOYEE(3, ' rAVI', 75000.00, 'IT', 'ACTIVE');

CREATE OR REPLACE PROCEDURE VITECH_DEV.PUBLIC.MARK_INACTIVE(P_EID INT)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'mark_inactive'
AS
$$
def mark_inactive(session, p_eid):
    session.sql(f"UPDATE VITECH_DEV.PUBLIC.EMPLOYEE SET STATUS = 'INACTIVE' WHERE EID = {p_eid}").collect()
    return f'Employee {p_eid} marked as INACTIVE'
$$;

CALL VITECH_DEV.PUBLIC.MARK_INACTIVE(2);

SELECT * FROM VITECH_DEV.PUBLIC.EMPLOYEE;

---

----write stored procedure to delete inactive employees every day 

--Bank example 
--Create db ,table name as customers 
-- write sp to insert customer data (account number , custmner name , address , phone , email,status , branch , amount)
--check blance based on account number   (if customer exist only , if not throw an error msg)
--Deposit amount based on account number , amount  (if customer exist only , if not throw an error msg)
--wihtdraw amount based on account number , amount  (if customer exist only , if not throw an error msg ,check balance sufficent /in)
--Dispaly all customers based on branch ..here use curosr (open , fetch ,close )




