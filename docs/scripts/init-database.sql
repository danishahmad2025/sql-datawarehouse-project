/*create database and schemas
script purpose:creating the database named 'datawarehouse' after checking if it already exists.
if it already exists then it drop it and recreate the database and within this scripts here are three schemas
'bronze','silver' and 'gold.'

WARNING: This action is destructive and irreversible — it will permanently delete the datawarehouse database and all its data. 
Be sure you have backups if needed. */




use master;
GO
-- Check if the database exists
IF EXISTS (
    SELECT name 
    FROM sys.databases 
    WHERE name = 'datawarehouse'
)
BEGIN
    -- Optional: Set the database to single-user mode to force disconnect users
    ALTER DATABASE datawarehouse
    SET SINGLE_USER
    WITH ROLLBACK IMMEDIATE;

    -- Drop the database
    DROP DATABASE datawarehouse;

    PRINT 'Database "datawarehouse" has been dropped.';
END;
--creating database
CREATE DATABASE datawarehouse;
GO
use datawarehouse;
GO
  --creating schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
