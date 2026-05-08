USE UK_Infrastructure;
GO
--                                    DATA CLEANING 

-- CREATING NEW SCHEMA (clean) -- to store each cleaned table

CREATE SCHEMA clean;
GO

/*============================================================================================================
CLEANING TABLE (region)
==============================================================================================================
PROCESS:
Data is mostly clean. Comma removed from numeric columns before casting to correct data_types.
String columns trimmed for whitespace.

raw table   -- dbo.region
clean table -- clean.region
=============================================================================================================*/

SELECT 
	TRY_CAST(region_id AS VARCHAR(10)) AS region_id,
	TRY_CAST(TRIM(region_name) AS VARCHAR(50)) AS region_name,
	TRY_CAST(country AS VARCHAR(20)) AS country,
	TRY_CAST(REPLACE(total_population_2021, ',', '') AS INT) AS total_population_2021,
	TRY_CAST(REPLACE(area_sq_km,',','') AS INT) AS area_sq_km,
	TRY_CAST(num_local_authorities AS INT) AS num_local_authorities,
	TRY_CAST(REPLACE(gdp_per_capita_GBP,',','') AS INT) AS gdp_per_capital_GBP

INTO clean.region    -- Inserting into new table
FROM dbo.region;





/*============================================================================================================
CLEANING TABLE (local_authorities)
==============================================================================================================
PROCESS:
Check and inspect duplicated rows to determine which records are deleted.
Check for white spaces in the (council_name) column before standardizing.
String columns are trimmed for whitespaces.
Removed comma in numeric columns before casting to correct data_type.
Removed duplicates using window functions to have control over which record is deleted.

raw table   -- dbo.local_authorities
clean table -- clean.local_authorities
===============================================================================================================*/

--check for duplicates and inspection of duplicates     
SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY council_id ORDER BY year_established DESC, population_2021 DESC) AS rn
FROM dbo.local_authorities
WHERE council_id IN
(
	SELECT 
		council_id
	FROM dbo.local_authorities
	GROUP BY council_id
	HAVING COUNT(*) > 1           -- Duplicate exists
)            


-- Check for white spaces in the council_name columns before taking care of anomaly (LEICESTER CITY COUNCIL)
SELECT                         
	TRIM(council_id) AS council_id,
	TRIM(council_name),
	LEN(TRIM(council_name)),
	LEN(council_name),
	LEN(council_name) - LEN(TRIM(council_name)) AS diff
FROM dbo.local_authorities
WHERE LEN(council_name) - LEN(TRIM(council_name)) <> 0   -- No whitespaces present in the column


-- CLEANING 

WITH duplicates_rank AS
(
SELECT 
	TRIM(council_id) AS council_id,
	CASE
		WHEN council_name = 'LEICESTER CITY COUNCIL' THEN 'Leicester City Council'  --only record not standardized
		ELSE council_name
	END AS council_name,
	TRIM(region_id) AS region_id,
	CASE
		WHEN LOWER(TRIM(region_name)) = 'east midlands'            THEN 'East Midlands'
		WHEN LOWER(TRIM(region_name)) = 'east of england'          THEN 'East of England'
		WHEN LOWER(TRIM(region_name)) = 'eastern england'          THEN 'East of England'
		WHEN LOWER(TRIM(region_name)) = 'london'                   THEN 'London'
		WHEN LOWER(TRIM(region_name)) = 'greater london'           THEN 'London'
		WHEN LOWER(TRIM(region_name)) = 'ne england'               THEN 'North East'
		WHEN LOWER(TRIM(region_name)) = 'north east'               THEN 'North East'
		WHEN LOWER(TRIM(region_name)) = 'nw england'               THEN 'North West'
		WHEN LOWER(TRIM(region_name)) = 'north west'               THEN 'North West'
		WHEN LOWER(TRIM(region_name)) = 'south east'               THEN 'South East'
		WHEN LOWER(TRIM(region_name)) = 'se england'               THEN 'South East'
		WHEN LOWER(TRIM(region_name)) = 'south west'               THEN 'South West'
		WHEN LOWER(TRIM(region_name)) = 'sw england'               THEN 'South West'
		WHEN LOWER(TRIM(region_name)) = 'west midlands'            THEN 'West Midlands'
		WHEN LOWER(TRIM(region_name)) = 'w midlands'               THEN 'West Midlands'
		WHEN LOWER(TRIM(region_name)) = 'yorkshire & humber'       THEN 'Yorkshire and The Humber'
		WHEN LOWER(TRIM(region_name)) = 'yorkshire and the humber' THEN 'Yorkshire and The Humber'
		ELSE TRIM(region_name)
	END AS region_name,              -- standardizing region_name
	TRIM(country) AS country,
	TRY_CAST(REPLACE(population_2021,',','') AS INT) AS population_2021,
	TRY_CAST(area_sq_km AS DECIMAL(10,1)) AS area_sq_km,
	TRIM(urban_rural_classification) AS urban_rural_classification,
	deprivation_band,
	TRY_CAST(imd_rank AS INT) AS imd_rank,
	CASE
		WHEN TRY_CAST(imd_rank AS INT) BETWEEN 1 AND 100   THEN 'Valid'
		WHEN imd_rank IS NULL                               THEN 'Missing'
		ELSE 'Invalid - Out of Range'
	END AS imd_rank_flag,
	TRY_CAST(year_established AS INT) AS year_established,
	ROW_NUMBER() OVER (PARTITION BY council_id ORDER BY year_established DESC, population_2021 DESC) AS rn
FROM dbo.local_authorities
)

SELECT
	council_id,
	council_name,
	region_id,
	region_name,
	country,
	population_2021,
	area_sq_km,
	urban_rural_classification,
	deprivation_band,
	imd_rank,
	year_established

INTO clean.local_authorities     -- Inserting into new table
FROM duplicates_rank
WHERE rn = 1                     -- duplicates removed, only relevant rows left





/*=============================================================================================================
CLEANING TABLE (infrastructure_spending)
===============================================================================================================
PROCESS:
Check for duplicates -- no duplicates found.
Using a CTE, strings are standardized and trimmed for white spaces.
Pound symbol and comma are removed from numeric columns before casting to correct data_types.
With another CTE, mathematical calculations were done to fill in NULLS numeric columns (budget,actual,variance and pct_budget_utilised).
Joined tables to extract an important column (population_2021) to help fill in NULLS in the spend_per_capita column.
COALESCE to return non-null values in the calculated columns

raw table   -- dbo.infrastructure_spending
clean table -- clean.infrastructure_spending
================================================================================================================*/

--Check for duplicates
SELECT 
	column1,
	COUNT(*) AS occurence
FROM dbo.infrastructure_spending
GROUP BY column1
HAVING COUNT(*) > 1               -- No duplicate records


-- Cleaning data
WITH standardized_spend AS 
(
SELECT 
	column1 AS spend_id,
	TRIM(council_id ) AS council_id,
	TRIM(council_name) AS council_name,
	CASE
		WHEN LOWER(TRIM(region_name)) = 'east midlands'            THEN 'East Midlands'
		WHEN LOWER(TRIM(region_name)) = 'east of england'          THEN 'East of England'
		WHEN LOWER(TRIM(region_name)) = 'eastern england'          THEN 'East of England'
		WHEN LOWER(TRIM(region_name)) = 'london'                   THEN 'London'
		WHEN LOWER(TRIM(region_name)) = 'greater london'           THEN 'London'
		WHEN LOWER(TRIM(region_name)) = 'ne england'               THEN 'North East'
		WHEN LOWER(TRIM(region_name)) = 'north east'               THEN 'North East'
		WHEN LOWER(TRIM(region_name)) = 'nw england'               THEN 'North West'
		WHEN LOWER(TRIM(region_name)) = 'north west'               THEN 'North West'
		WHEN LOWER(TRIM(region_name)) = 'south east'               THEN 'South East'
		WHEN LOWER(TRIM(region_name)) = 'se england'               THEN 'South East'
		WHEN LOWER(TRIM(region_name)) = 'south west'               THEN 'South West'
		WHEN LOWER(TRIM(region_name)) = 'sw england'               THEN 'South West'
		WHEN LOWER(TRIM(region_name)) = 'west midlands'            THEN 'West Midlands'
		WHEN LOWER(TRIM(region_name)) = 'w midlands'               THEN 'West Midlands'
		WHEN LOWER(TRIM(region_name)) = 'yorkshire & humber'       THEN 'Yorkshire and The Humber'
		WHEN LOWER(TRIM(region_name)) = 'yorkshire and the humber' THEN 'Yorkshire and The Humber'
		ELSE TRIM(region_name)
	END AS region_name,              -- standardizing region_name
	TRY_CAST(spend_year AS INT) AS spend_year,
	TRIM(spend_category) AS spend_category,
	TRY_CAST(REPLACE(REPLACE(budget_GBP,'£',''),',','') AS DECIMAL(18,2)) AS budget_GBP,
	TRY_CAST(REPLACE(REPLACE(actual_spend_GBP, '£', ''),',','') AS DECIMAL(18,2)) AS actual_spend_GBP,
	TRY_CAST(variance_GBP AS DECIMAL(18,2)) AS variance_GBP,
	TRY_CAST(spend_per_capita_GBP AS DECIMAL(10,2)) AS spend_per_capita_GBP,
	TRY_CAST(pct_budget_utilised AS DECIMAL(10,1)) AS pct_budget_utilised,
	TRIM(funding_source) AS funding_source
FROM dbo.infrastructure_spending
),
--Mathematical calculations to update nulls in budget,actual,variance,pct_budget_utilized and spend_per_capita
mathematical_calc AS 
(
SELECT
	spend_id,
	council_id,
	council_name,
	region_name,
	spend_year,
	spend_category,
	budget_GBP,
	CASE
		WHEN actual_spend_GBP    IS NOT NULL AND variance_GBP        IS NOT NULL THEN TRY_CAST(actual_spend_GBP AS FLOAT) - variance_GBP
		WHEN actual_spend_GBP    IS NOT NULL AND pct_budget_utilised IS NOT NULL THEN ROUND((TRY_CAST(actual_spend_GBP AS FLOAT) * 100 / pct_budget_utilised),2)
		WHEN variance_GBP        IS NOT NULL AND pct_budget_utilised IS NOT NULL THEN ROUND((100 * TRY_CAST(variance_GBP AS FLOAT)) /	NULLIF(pct_budget_utilised - 100,0),2)
		ELSE budget_GBP
	END AS calc_budget,
	actual_spend_GBP,
	CASE
		WHEN budget_GBP          IS NOT NULL AND variance_GBP        IS NOT NULL THEN budget_GBP + variance_GBP
		WHEN budget_GBP          IS NOT NULL AND pct_budget_utilised IS NOT NULL THEN ROUND((TRY_CAST(budget_GBP AS FLOAT) * pct_budget_utilised / 100),2)
		WHEN variance_GBP        IS NOT NULL AND pct_budget_utilised IS NOT NULL THEN ROUND((pct_budget_utilised * TRY_CAST(variance_GBP AS FLOAT)) / (pct_budget_utilised - 100),2)
		ELSE actual_spend_GBP
	END AS calc_actual_spend,
	variance_GBP,
	CASE
		WHEN budget_GBP          IS NOT NULL AND actual_spend_GBP    IS NOT NULL THEN actual_spend_GBP - budget_GBP
		WHEN pct_budget_utilised IS NOT NULL AND actual_spend_GBP    IS NOT NULL THEN ROUND((TRY_CAST(actual_spend_GBP AS FLOAT) * (pct_budget_utilised - 100)) / (pct_budget_utilised ),2)
		WHEN pct_budget_utilised IS NOT NULL AND budget_GBP          IS NOT NULL THEN ROUND(TRY_CAST(budget_GBP AS FLOAT) * (pct_budget_utilised - 100) / 100,2)
		ELSE variance_GBP
	END AS calc_variance,
	pct_budget_utilised,
	CASE
		WHEN budget_GBP          IS NOT NULL AND actual_spend_GBP    IS NOT NULL THEN ROUND((actual_spend_GBP * 100) / TRY_CAST(budget_GBP AS FLOAT),1)
		WHEN budget_GBP          IS NOT NULL AND variance_GBP        IS NOT NULL THEN ROUND((100 * variance_GBP / TRY_CAST(budget_GBP AS FLOAT)) + 100,1)
		WHEN actual_spend_GBP    IS NOT NULL AND variance_GBP        IS NOT NULL THEN ROUND((100 * TRY_CAST(actual_spend_GBP AS FLOAT)) / (actual_spend_GBP - variance_GBP),1)
		ELSE pct_budget_utilised 
	END AS calc_pct_budg_utilized,
	spend_per_capita_GBP,
	funding_source
FROM standardized_spend 
)

SELECT 
	m.spend_id,
	m.council_id,
	m.council_name,
	m.region_name,
	m.spend_year,
	m.spend_category,
	COALESCE(m.budget_GBP,         m.calc_budget)           AS budget_GBP,
	COALESCE(m.actual_spend_GBP,   m.calc_actual_spend)     AS actual_spend_GBP,
	COALESCE(m.variance_GBP,       m.calc_variance)         AS variance_GBP,
	COALESCE(m.pct_budget_utilised,m.calc_pct_budg_utilized)AS pct_budget_utilised,
	COALESCE(m.spend_per_capita_GBP, 
		CASE WHEN m.calc_actual_spend IS NOT NULL 
			  AND l.population_2021   IS NOT NULL 
		THEN ROUND(m.calc_actual_spend / l.population_2021, 2)
		ELSE NULL END)                                       AS spend_per_capita_GBP,
	m.funding_source

INTO clean.infrastructure_spending      --Inserting into new table
FROM mathematical_calc m
LEFT JOIN clean.local_authorities l
ON m.council_id = l.council_id





/*=============================================================================================================
CLEANING TABLE (road_conditions)
===============================================================================================================
PROCESS:
Check for duplicates -- No dupliccate record found.
String values are standardized (CASE WHEN) then trimmed for white spaces.
Columns are casted into correct data_types.
COALESCE is used to handle really messy date and the formatted into UK style.
Flag column is used to indicate missing,valid and wrongly inputted survey datas.
Average score formula is used to fill NUlls in the average score column.

raw table   -- dbo.road_conditions
clean table -- clean.road_conditions
================================================================================================================*/

--Check for duplicates         -- No duplicates found
SELECT 
	record_id,
	COUNT(*) AS occurence
FROM dbo.road_conditions
GROUP BY record_id
HAVING COUNT(*) > 1

--   CLEANING
WITH condition AS
(
SELECT
	TRIM(record_id) AS record_id,
	TRIM(council_id) AS council_id,
	TRIM(council_name) AS council_name,
	CASE
		WHEN LOWER(TRIM(region_name)) = 'east midlands'            THEN 'East Midlands'
		WHEN LOWER(TRIM(region_name)) = 'east of england'          THEN 'East of England'
		WHEN LOWER(TRIM(region_name)) = 'eastern england'          THEN 'East of England'
		WHEN LOWER(TRIM(region_name)) = 'london'                   THEN 'London'
		WHEN LOWER(TRIM(region_name)) = 'greater london'           THEN 'London'
		WHEN LOWER(TRIM(region_name)) = 'ne england'               THEN 'North East'
		WHEN LOWER(TRIM(region_name)) = 'north east'               THEN 'North East'
		WHEN LOWER(TRIM(region_name)) = 'nw england'               THEN 'North West'
		WHEN LOWER(TRIM(region_name)) = 'north west'               THEN 'North West'
		WHEN LOWER(TRIM(region_name)) = 'south east'               THEN 'South East'
		WHEN LOWER(TRIM(region_name)) = 'se england'               THEN 'South East'
		WHEN LOWER(TRIM(region_name)) = 'south west'               THEN 'South West'
		WHEN LOWER(TRIM(region_name)) = 'sw england'               THEN 'South West'
		WHEN LOWER(TRIM(region_name)) = 'west midlands'            THEN 'West Midlands'
		WHEN LOWER(TRIM(region_name)) = 'w midlands'               THEN 'West Midlands'
		WHEN LOWER(TRIM(region_name)) = 'yorkshire & humber'       THEN 'Yorkshire and The Humber'
		WHEN LOWER(TRIM(region_name)) = 'yorkshire and the humber' THEN 'Yorkshire and The Humber'
		ELSE TRIM(region_name)
	END AS region_name,              -- standardizing region_name
	TRY_CAST(survey_year AS INT) AS survey_year,
	TRIM(road_type) AS road_type,
	TRY_CAST(total_road_length_km AS DECIMAL(10,2)) AS total_road_length_km,
	TRY_CAST(pct_good_condition AS DECIMAL(10,2)) AS pct_good_condition,
	TRY_CAST(pct_satisfactory AS DECIMAL(10,2)) AS pct_satisfactory,
	TRY_CAST(pct_poor AS DECIMAL(10,2)) AS pct_poor,
	TRY_CAST(pct_very_poor AS DECIMAL(10,2)) AS pct_very_poor,
	(TRY_CAST(pct_good_condition AS DECIMAL(10,2)) + TRY_CAST(pct_satisfactory AS DECIMAL(10,2)) + TRY_CAST(pct_poor AS DECIMAL(10,2)) + TRY_CAST(pct_very_poor AS DECIMAL(10,2))) AS total_pct_cond,
	avg_condition_score,
	TRY_CAST(maintenance_backlog_GBP AS DECIMAL(18,2)) AS maintenance_backlog_GBP,
	COALESCE
    ( 
		TRY_CONVERT(DATE,survey_date,23),
		TRY_CONVERT(DATE,survey_date,103),     -- priotize UK style since working with official UK data
		TRY_CONVERT(DATE,survey_date,101),
		TRY_CONVERT(DATE,survey_date,107),
		TRY_CONVERT(DATE,survey_date,106)
    ) AS survey_date
FROM dbo.road_conditions
)

SELECT 
	record_id,
	council_id,
	council_name,
	region_name,
	survey_year,
	road_type,
	total_road_length_km,
	pct_good_condition,
	pct_satisfactory,
	pct_poor,
	pct_very_poor,
	CASE
		WHEN pct_good_condition IS NULL 
		  OR pct_satisfactory   IS NULL
		  OR pct_poor           IS NULL
		  OR pct_very_poor      IS NULL      THEN 'Incomplete Data'
		WHEN total_pct_cond <> 100          THEN 'Bad Survey Input'
		ELSE                                     'Good'
	END AS survey_quality_flag,
	ROUND(CASE
			  WHEN avg_condition_score IS NULL THEN ((pct_good_condition * 1.0) + (pct_satisfactory * 0.6) + (pct_poor * 0.25) + (pct_very_poor * 0)) / 100
		      ELSE avg_condition_score
		  END,3) AS avg_condition_score,
	maintenance_backlog_GBP,
	survey_date AS survey_date

INTO clean.road_conditions      --Inserting into new table
FROM condition





/*=============================================================================================================
CLEANING TABLE (deprivation_score)
===============================================================================================================
PROCESS:
Check for duplicates.
String values are standardized (CASE WHEN) and trimmed for white spaces.
Numeric values are casted into correct data_type.
Flag column is used to indicate missing,valid and invalid datas.

raw table   -- dbo.deprivation_score
clean table -- clean.deprivation_score
================================================================================================================*/

--check for duplicates
SELECT
	council_id,
	COUNT(*) AS occurence
FROM dbo.deprivation_score
GROUP BY council_id
HAVING COUNT(*) > 1            -- No duplicate record



--   CLEANING DATA
SELECT 
	TRIM(council_id) AS council_id,
	TRIM(council_name) AS council_name,
	CASE
		WHEN LOWER(TRIM(region_name)) = 'east midlands'            THEN 'East Midlands'
		WHEN LOWER(TRIM(region_name)) = 'east of england'          THEN 'East of England'
		WHEN LOWER(TRIM(region_name)) = 'eastern england'          THEN 'East of England'
		WHEN LOWER(TRIM(region_name)) = 'london'                   THEN 'London'
		WHEN LOWER(TRIM(region_name)) = 'greater london'           THEN 'London'
		WHEN LOWER(TRIM(region_name)) = 'ne england'               THEN 'North East'
		WHEN LOWER(TRIM(region_name)) = 'north east'               THEN 'North East'
		WHEN LOWER(TRIM(region_name)) = 'nw england'               THEN 'North West'
		WHEN LOWER(TRIM(region_name)) = 'north west'               THEN 'North West'
		WHEN LOWER(TRIM(region_name)) = 'south east'               THEN 'South East'
		WHEN LOWER(TRIM(region_name)) = 'se england'               THEN 'South East'
		WHEN LOWER(TRIM(region_name)) = 'south west'               THEN 'South West'
		WHEN LOWER(TRIM(region_name)) = 'sw england'               THEN 'South West'
		WHEN LOWER(TRIM(region_name)) = 'west midlands'            THEN 'West Midlands'
		WHEN LOWER(TRIM(region_name)) = 'w midlands'               THEN 'West Midlands'
		WHEN LOWER(TRIM(region_name)) = 'yorkshire & humber'       THEN 'Yorkshire and The Humber'
		WHEN LOWER(TRIM(region_name)) = 'yorkshire and the humber' THEN 'Yorkshire and The Humber'
		ELSE TRIM(region_name)
	END AS region_name,              -- standardizing region_name
	TRY_CAST(reference_year AS INT) AS reference_year,
	TRY_CAST(overall_imd_score AS DECIMAL(5,2)) AS overall_imd_score,
	CASE
		WHEN TRY_CAST(overall_imd_score AS DECIMAL(5,2)) BETWEEN 0 AND 100 THEN 'Valid'
		WHEN overall_imd_score IS NULL                                     THEN 'Missing'
		ELSE                                                                    'Invalid'
	END AS imd_score_quality_flag,
	TRY_CAST(overall_imd_rank AS INT) AS overall_imd_rank,
	CASE
		WHEN TRY_CAST(overall_imd_rank AS INT) BETWEEN 1 AND 100 THEN 'Valid'
		WHEN overall_imd_rank IS NULL                            THEN 'Missing'
		ELSE                                                          'Invalid'
	END AS imd_rank_quality_flag,
	TRIM(deprivation_band) AS deprivation_band,
	TRY_CAST(income_deprivation AS DECIMAL(10,2)) AS income_deprivation,
	TRY_CAST(employment_deprivation AS DECIMAL(5,2)) AS employment_deprivation,
	TRY_CAST(education_skills_and_trainin AS DECIMAL(5,2)) AS education_skills_and_trainin,
	TRY_CAST(health_deprivation_and_disab AS DECIMAL(5,2)) AS health_deprivation_and_disab,
	TRY_CAST(crime AS DECIMAL(5,2)) AS crime,
	TRY_CAST(barriers_to_housing_and_serv AS DECIMAL(5,2)) AS barriers_to_housing_and_serv,
	TRY_CAST(living_environment AS DECIMAL(5,2)) AS living_environment,
	TRY_CAST(children_and_young_people AS DECIMAL(5,2)) AS children_and_young_people,
	TRY_CAST(older_people AS DECIMAL(5,2)) AS older_people,
	TRY_CAST(geographic_access_to_service AS DECIMAL(5,2)) AS geographic_access_to_service

INTO clean.deprivation_score
FROM dbo.deprivation_score





/*====================================================================================================================
CLEANING TABLE (projects_register)
======================================================================================================================
PROCESS:

    STAGE 1 -- Using CTE
		- Check for duplicates. --No duplicate records found.
		- String values are standardized and trimmed for white spaces.
		- Messy date columns are cleaned and converted into correct data_types (DATE).
		- Commas and pounds symbols were removed from numeric columns before casting into correct data_types.
		- Maathematical calculations to fill in Nulls in numeric columns 
		
	STAGE 2 -- Using a staging table (dbo.proj_regis_unfinished)
		- Serious data quality checks were conducted.
		- Derived new data columns (data transformation).
		- Flagged invalid findings.

raw table     -- dbo.projects_register
staging table -- dbo.proj_regis_unfinished
clean table   -- clean.projects_register
===================================================================================================================*/

-- check for duplicates
SELECT 
	project_id,
	COUNT(*) AS occurence
FROM dbo.projects_register
GROUP BY project_id
HAVING COUNT(*) > 1         --No duplicate record


--   CLEANING
WITH first_step AS 
(
SELECT 
	TRIM(project_id) AS project_id,
	TRIM(council_id) AS council_id,
	TRIM(council_name) AS council_name,
	CASE
		WHEN LOWER(TRIM(region_name)) = 'east midlands'            THEN 'East Midlands'
		WHEN LOWER(TRIM(region_name)) = 'east of england'          THEN 'East of England'
		WHEN LOWER(TRIM(region_name)) = 'eastern england'          THEN 'East of England'
		WHEN LOWER(TRIM(region_name)) = 'london'                   THEN 'London'
		WHEN LOWER(TRIM(region_name)) = 'greater london'           THEN 'London'
		WHEN LOWER(TRIM(region_name)) = 'ne england'               THEN 'North East'
		WHEN LOWER(TRIM(region_name)) = 'north east'               THEN 'North East'
		WHEN LOWER(TRIM(region_name)) = 'nw england'               THEN 'North West'
		WHEN LOWER(TRIM(region_name)) = 'north west'               THEN 'North West'
		WHEN LOWER(TRIM(region_name)) = 'south east'               THEN 'South East'
		WHEN LOWER(TRIM(region_name)) = 'se england'               THEN 'South East'
		WHEN LOWER(TRIM(region_name)) = 'south west'               THEN 'South West'
		WHEN LOWER(TRIM(region_name)) = 'sw england'               THEN 'South West'
		WHEN LOWER(TRIM(region_name)) = 'west midlands'            THEN 'West Midlands'
		WHEN LOWER(TRIM(region_name)) = 'w midlands'               THEN 'West Midlands'
		WHEN LOWER(TRIM(region_name)) = 'yorkshire & humber'       THEN 'Yorkshire and The Humber'
		WHEN LOWER(TRIM(region_name)) = 'yorkshire and the humber' THEN 'Yorkshire and The Humber'
		ELSE TRIM(region_name)
	END AS region_name,              -- standardizing region_name
	TRIM(project_type) AS project_type,
	TRIM(project_name) AS project_name,
	COALESCE (
			 TRY_CONVERT(DATE,start_date,23),
			 TRY_CONVERT(DATE,start_date,103),      -- priotize UK style since working with official UK data
			 TRY_CONVERT(DATE,start_date,101),
			 TRY_CONVERT(DATE,start_date,107),
			 TRY_CONVERT(DATE,start_date,106)
		     ) AS start_date,
	COALESCE (
			 TRY_CONVERT(DATE,planned_end_date,23),
			 TRY_CONVERT(DATE,planned_end_date,103),    -- priotize UK style since working with official UK data
			 TRY_CONVERT(DATE,planned_end_date,101),
			 TRY_CONVERT(DATE,planned_end_date,107),
			 TRY_CONVERT(DATE,planned_end_date,106)
		     ) AS planned_end_date,
	COALESCE (
			 TRY_CONVERT(DATE,actual_end_date,23),
			 TRY_CONVERT(DATE,actual_end_date,103),   -- priotize UK style since working with official UK data
			 TRY_CONVERT(DATE,actual_end_date,101),
			 TRY_CONVERT(DATE,actual_end_date,107),
			 TRY_CONVERT(DATE,actual_end_date,106)
		     ) AS actual_end_date,
	TRY_CAST(REPLACE(REPLACE(budget_GBP,',',''),'£','') AS DECIMAL(18,2)) AS budget_GBP,
	TRY_CAST(REPLACE(REPLACE(actual_cost_GBP,',',''),'£','') AS DECIMAL(18,2)) AS actual_cost_GBP,
	TRY_CAST(cost_variance_GBP AS DECIMAL(18,2)) AS cost_variance_GBP,
    TRY_CAST(planned_duration_months AS INT) AS planned_duration_months,
	TRY_CAST(actual_duration_months AS INT) AS actual_duration_months,
	TRY_CAST(delay_months AS INT) AS delay_months,
	TRIM(status) AS status,
	TRIM(primary_delay_reason) AS primary_delay_reason,
	TRIM(contractor) AS contractor,
	TRIM(consultant) AS consultant,
	TRY_CAST(pct_complete AS INT) AS pct_complete
FROM dbo.projects_register
)

SELECT 
	project_id,
	council_id,
	council_name,
	region_name,
	project_type,
	project_name,
	start_date,
	planned_end_date,
	actual_end_date,
	budget_GBP,
	actual_cost_GBP,
	cost_variance_GBP,
	planned_duration_months,
	CASE
		WHEN planned_duration_months IS NOT NULL AND delay_months IS NOT NULL THEN planned_duration_months + delay_months
		ELSE actual_duration_months
	END AS actual_duration_months,
	CASE
		WHEN planned_duration_months IS NOT NULL AND actual_duration_months IS NOT NULL THEN actual_duration_months - planned_duration_months
		ELSE delay_months
	END AS delay_months,
	status,
	primary_delay_reason,
	contractor,
	consultant,
	pct_complete

INTO dbo.proj_regis_unfinished        -- staging table           --- to be dropped later
FROM first_step


                        -- Data Validations       -- Quality checks 

-- checking End date before start date (impossible dates)
SELECT
	project_id,
	start_date,
	planned_end_date
FROM dbo.proj_regis_unfinished
WHERE planned_end_date < start_date                  --Impossible dates     


-- checking planned_duration_months VS date_gap
WITH dur_diference_t AS 
(
SELECT
	project_id,
	start_date,
	planned_end_date,
	DATEDIFF(MONTH,start_date,planned_end_date) AS derived_planned_dur_months,
	planned_duration_months,
	ABS(planned_duration_months - (DATEDIFF(MONTH,start_date,planned_end_date))) AS duration_diff
FROM dbo.proj_regis_unfinished
)

SELECT 
	project_id,
	start_date,
	planned_end_date,
	duration_diff,
	CASE
		WHEN start_date IS NULL OR planned_end_date IS NULL THEN 'Missing Date'
		WHEN planned_end_date < start_date                  THEN 'Impossible Date'
		WHEN duration_diff <= 1                             THEN 'Acceptable'
		WHEN duration_diff BETWEEN 2 AND 3                  THEN 'Review Recommended'
		ELSE                                                     'Date Error'
	END  AS duration_diff_flag
FROM dur_diference_t


--checking actual_end_date when status = 'planning'        -- retured result set should be empty
SELECT
	project_id,
	actual_end_date,
	status
FROM dbo.proj_regis_unfinished
WHERE LOWER(status) = 'planning'  AND                                
      actual_end_date IS NOT NULL                          -- valid data 



--checking pct_complete when status = 'planning'           --  retured result set should be empty
SELECT
	project_id,
	pct_complete,
	status
FROM dbo.proj_regis_unfinished
WHERE LOWER(status) = 'planning'  AND                                
      pct_complete  > 0                                -- Invalid data, project in planning phase cannot have a pct_complete data entry.


--checking projects with impossible pct_complete  values
SELECT
	project_id,
	pct_complete
FROM dbo.proj_regis_unfinished
WHERE pct_complete < 0   OR 
      pct_complete > 100                                 -- Impossible values present



--checking Completed projects with NULL actual_end_date or planned_duration_months        --retured result set should be empty     
SELECT
	project_id,
	actual_end_date,
	actual_duration_months,
	status
FROM dbo.proj_regis_unfinished
WHERE LOWER(status) = 'completed' AND
      (actual_end_date IS NULL OR planned_duration_months IS NULL)                       -- valid data 



-- council_ids in projects_register that don't exist in local_authorities
SELECT
    p.project_id,
    p.council_id,
    p.council_name
FROM dbo.proj_regis_unfinished p
LEFT JOIN clean.local_authorities l
    ON p.council_id = l.council_id
WHERE l.council_id IS NULL                                     -- Broken Foreign Keys (This projects cant be joined)


/*====================================================================================================================
BEFORE FINAL CLEANING:
1. Quality checks has been conducted on all columns and with my findings the planned_end_date is not a reliable data column,
therefore i will be doing data transformation (derive new column) to help in further analysis using the planned_duration_months.

2. All other invalid findings will be flagged accordingly.
======================================================================================================================*/

--Final cleaning process and flagging inappropriate values

SELECT
	p.project_id,
	p.council_id,
	CASE
		WHEN l.council_id IS NULL THEN 'Invalid - Broken FK'
		ELSE 'Valid'
	END AS council_fk_flag,
	p.council_name,
	p.region_name,
	project_type,
	project_name,
	start_date,
	planned_end_date,        -- Will not be using column for analysis.
	DATEADD(MONTH,planned_duration_months,start_date) AS derived_planned_end_date, -- working with this instead
	actual_end_date,
	DATEADD(MONTH,actual_duration_months,start_date)  AS derived_actual_end_date,
	budget_GBP,
	actual_cost_GBP,
	cost_variance_GBP,
	planned_duration_months,
	actual_duration_months,
	delay_months,
	CASE
		WHEN LOWER(status) = 'delayed'   THEN 'Delayed'
		WHEN LOWER(status) = 'planning'  THEN 'Planning'
		WHEN LOWER(status) = 'completed' THEN 'Completed'
		WHEN LOWER(status) = 'ongoing'   THEN 'Ongoing'
		ELSE status
	END AS status,
	CASE
		WHEN LOWER(status) = 'planning'  AND actual_duration_months IS NOT NULL     THEN 'Invalid - Actual Duration in Planning'
		WHEN LOWER(status) = 'planning'  AND actual_end_date        IS NOT NULL     THEN 'Invalid - Actual End date in Planning'
		WHEN LOWER(status) = 'completed' AND actual_end_date        IS NULL         THEN 'Incomplete Data Entry'
		ELSE   'Valid'
	END AS status_duration_flag,
	primary_delay_reason,
	contractor,
	consultant,
	pct_complete,
	CASE
		WHEN pct_complete BETWEEN 0 AND 100 THEN 'Valid'
		WHEN pct_complete IS NULL           THEN 'Unknown'
		ELSE 'Invalid'
	END AS pct_complete_flag

INTO clean.projects_register
FROM dbo.proj_regis_unfinished p
LEFT JOIN clean.local_authorities l
    ON p.council_id = l.council_id

DROP TABLE dbo.proj_regis_unfinished         -- dropping staging table
