-- Create layer schemas (Medallion: bronze, silver, gold). Run after 001_init_datawarehouse.sql.
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
