
-- Create a database
-- DROP DATABASE IF EXISTS batchstartdb;
CREATE DATABASE batchstartdb;
USE batchstartdb;

SET SQL_SAFE_UPDATES = 0;

-- Create a table and insert rows
DROP TABLE IF EXISTS batch_control_tb;
CREATE TABLE batch_control_tb (
    curr_dt DATE NOT NULL,
    prev_dt DATE NOT NULL,
    curr_ind int NOT NULL,
    updated_dt timestamp,
    CONSTRAINT UC_Date UNIQUE (curr_dt,prev_dt)
);


insert into batch_control_tb values ('2020-08-06','2020-08-05',1,sysdate());
commit;
-- Read
SELECT * batch_control_tb;


-- Create a table and insert rows
DROP TABLE IF EXISTS job_status_tb;
CREATE TABLE job_status_tb (
    job_id varchar(255) NOT NULL PRIMARY KEY,
    status varchar(255) NOT NULL,
    updated_time timestamp
);



