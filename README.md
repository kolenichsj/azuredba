# azuredba
Script with some functions for leveraging Azure for SQL Server operations

Script was built to use the Az.Storage module and dbatools module for operations using SQL Servers hosted on Azure PAAS. When written, 
backup file names were based on namin convention of SQL Server Backup (https://ola.hallengren.com). There are some functions for parsing backup files names
and retrieving from blob storage.
