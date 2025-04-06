/*
===================
Create Database and Schemas
===================

Script purpose:
This script creates a new database named 'DataWarehouse' after checking if it already exists. 
If this database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

Warning:
	Running the script will drop the entire 'DataWarehouse' database if it exists,
	All data in the database will be peemamently deleted. Proceed with caution and ensure you have proper backups before running this script.

*/


USE master;

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
	END;
	GO


CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
