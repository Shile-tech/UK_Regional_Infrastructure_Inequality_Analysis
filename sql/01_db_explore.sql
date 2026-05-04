USE UK_Infrastructure;

/*============================================================================================
 DATABASE EXPLORATION
  Scanning through each table to familiarize with the data and know where possible errors might be in 
  other to help during data cleaning
  --============================================================================================*/

SELECT TOP 10 * 
FROM dbo.region;

SELECT TOP 10 * 
FROM dbo.local_authorities;

SELECT TOP 10 * 
FROM dbo.infrastructure_spending;

SELECT TOP 10 * 
FROM dbo.road_conditions;

SELECT TOP 10 * 
FROM dbo.deprivation_score;

SELECT TOP 10 * 
FROM dbo.projects_register;
