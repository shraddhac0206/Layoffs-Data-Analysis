-- ==========================================
-- SQL Data Cleaning & Exploratory Analysis
-- Dataset: https://www.kaggle.com/datasets/swaptr/layoffs-2022
-- ==========================================

-- STEP 1: Create a staging table from raw data
CREATE TABLE layoffs_staging AS
SELECT * FROM layoffs;

-- Optional (if not already done): populate from original
INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

-- STEP 2: Detect & Remove Duplicate Records

SELECT *
FROM world_layoffs.layoffs_staging
;
-- First let's check for duplicate enteries
-- Add a row number to identify duplicates (exact matches on key columns)
SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging;


SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- Let's have a look at Company Oda
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda';

-- It looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate in this case.
-- We find duplicate enteries with all the columns included.

SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- Now, these are the ones which we want to delete where the row number is > 1 or 2 or greater.

CREATE TABLE layoffs_deduped AS
SELECT *,
  ROW_NUMBER() OVER (
    PARTITION BY company, location, date, total_laid_off, percentage_laid_off, industry, stage, country, funds_raised
  ) AS row_num
FROM layoffs_staging;

-- Disable safe update mode to allow mass deletion
SET SQL_SAFE_UPDATES = 0;

-- Delete all rows that are duplicates (row_num > 1)
DELETE FROM layoffs_deduped
WHERE row_num > 1;

-- Drop the helper column now that duplicates are removed
ALTER TABLE layoffs_deduped DROP COLUMN row_num;

-- STEP 3: Clean and standardize data

SELECT *
FROM world_layoffs.layoffs_deduped;

-- Identify different `industry` column
SELECT DISTINCT industry
FROM world_layoffs.layoffs_deduped
ORDER BY industry;

-- Identify missing values in the `industry` column
SELECT *
FROM world_layoffs.layoffs_deduped
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Set empty strings to NULL
UPDATE layoffs_deduped
SET industry = NULL
WHERE industry = '';

SELECT *
FROM world_layoffs.layoffs_deduped
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- We find that Appsmith has a single entry and the industry in NULL so we assign 'Unknown' to it.
SELECT *
FROM world_layoffs.layoffs_deduped
WHERE company LIKE 'Appsmith%';

-- For companies with only 1 row and still NULL, label as 'Unknown'
UPDATE layoffs_deduped
SET industry = 'Unknown'
WHERE company = 'Appsmith' AND industry IS NULL;

SELECT DISTINCT industry
FROM world_layoffs.layoffs_deduped
ORDER BY industry;

-- Identify missing values in the `country` column
SELECT DISTINCT country
FROM world_layoffs.layoffs_deduped
ORDER BY country;

-- All the columns seems to be fine here.

-- STEP 4: Convert strings to numeric values

SELECT *
FROM world_layoffs.layoffs_deduped;

-- 4A: Remove $ from funds_raised and convert
UPDATE layoffs_deduped
SET funds_raised = REPLACE(funds_raised, '$', '');

UPDATE layoffs_deduped
SET funds_raised = NULL
WHERE TRIM(funds_raised) = '';

ALTER TABLE layoffs_deduped
MODIFY COLUMN funds_raised INT;

-- 4B: Clean percentage_laid_off (remove % and convert to DECIMAL)

SELECT *
FROM world_layoffs.layoffs_deduped;

UPDATE layoffs_deduped
SET percentage_laid_off = REPLACE(percentage_laid_off, '%', '');

UPDATE layoffs_deduped
SET percentage_laid_off = NULL
WHERE TRIM(percentage_laid_off) = '';

ALTER TABLE layoffs_deduped
MODIFY COLUMN percentage_laid_off DECIMAL(5,2);

-- 4C: Convert total_laid_off to DECIMAL

SELECT *
FROM world_layoffs.layoffs_deduped;

UPDATE layoffs_deduped
SET total_laid_off = NULL
WHERE TRIM(total_laid_off) = '';

ALTER TABLE layoffs_deduped
MODIFY COLUMN total_laid_off DECIMAL(10,2);

SELECT *
FROM world_layoffs.layoffs_deduped;

-- STEP 5: Fix date formats

-- Convert main layoff date
UPDATE layoffs_deduped
SET date = STR_TO_DATE(date, '%m/%d/%Y');

ALTER TABLE layoffs_deduped
MODIFY COLUMN date DATE;

-- Convert date_added
UPDATE layoffs_deduped
SET date_added = STR_TO_DATE(date_added, '%m/%d/%Y');

ALTER TABLE layoffs_deduped
MODIFY COLUMN date_added DATE;

SELECT *
FROM world_layoffs.layoffs_deduped;

-- STEP 6: Delete unusable rows (both layoffs & percentage missing)
DELETE FROM layoffs_deduped
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- FINAL REVIEW
SELECT * FROM layoffs_deduped LIMIT 100;


-- EDA: Exploring Cleaned Layoff Dataset


-- 1️ Basic Overview
SELECT * FROM layoffs_deduped LIMIT 100;

-- Total number of layoffs in the dataset
SELECT SUM(total_laid_off) AS total_layoffs
FROM layoffs_deduped;

-- Earliest and latest layoff dates
SELECT MIN(date) AS first_layoff_date, MAX(date) AS last_layoff_date
FROM layoffs_deduped;

-- Total number of companies
SELECT COUNT(DISTINCT company) AS total_companies
FROM layoffs_deduped;

-- Total affected countries
SELECT COUNT(DISTINCT country) AS countries_affected
FROM layoffs_deduped;

-- 2️ Extremes and Notable Cases

-- Maximum number of layoffs in a single event
SELECT *
FROM layoffs_deduped
ORDER BY total_laid_off DESC
LIMIT 5;

-- Maximum percentage layoffs (near or exactly 100%)
SELECT *
FROM layoffs_deduped
WHERE percentage_laid_off = 100;

-- Companies with most total layoffs (all-time)
SELECT company, SUM(total_laid_off) AS total
FROM layoffs_deduped
GROUP BY company
ORDER BY total DESC
LIMIT 10;

-- 3 Group-Level Aggregations

-- Layoffs by Country
SELECT country, SUM(total_laid_off) AS total
FROM layoffs_deduped
GROUP BY country
ORDER BY total DESC;

-- Layoffs by Location
SELECT location, SUM(total_laid_off) AS total
FROM layoffs_deduped
GROUP BY location
ORDER BY total DESC
LIMIT 10;

-- Layoffs by Industry
SELECT industry, SUM(total_laid_off) AS total
FROM layoffs_deduped
GROUP BY industry
ORDER BY total DESC;

-- Layoffs by Funding Stage
SELECT stage, SUM(total_laid_off) AS total
FROM layoffs_deduped
GROUP BY stage
ORDER BY total DESC;

-- Layoffs by Year
SELECT YEAR(date) AS year, SUM(total_laid_off) AS total
FROM layoffs_deduped
GROUP BY year
ORDER BY year ASC;

-- Layoffs by Month
SELECT DATE_FORMAT(date, '%Y-%m') AS month, SUM(total_laid_off) AS total
FROM layoffs_deduped
GROUP BY month
ORDER BY month;

-- 4️ Rolling Layoffs Trend (Cumulative)
WITH Monthly_Layoffs AS (
  SELECT DATE_FORMAT(date, '%Y-%m') AS month, SUM(total_laid_off) AS total
  FROM layoffs_deduped
  GROUP BY month
)
SELECT 
  month,
  total,
  SUM(total) OVER (ORDER BY month) AS rolling_total
FROM Monthly_Layoffs
ORDER BY month;

-- 5️ Top Companies Per Year (Layoff Volume)
WITH Company_Year AS (
  SELECT company, YEAR(date) AS year, SUM(total_laid_off) AS total
  FROM layoffs_deduped
  GROUP BY company, YEAR(date)
),
Ranked AS (
  SELECT company, year, total,
         DENSE_RANK() OVER (PARTITION BY year ORDER BY total DESC) AS ranks
  FROM Company_Year
)
SELECT * 
FROM Ranked
WHERE ranks <= 3
ORDER BY year ASC, ranks;

-- 6️ Who Raised the Most Funding But Still Laid Off 100%?
SELECT company, funds_raised, total_laid_off
FROM layoffs_deduped
WHERE percentage_laid_off = 100
ORDER BY funds_raised DESC;

-- 7️ Companies With Partial Layoffs (25%–75%)
SELECT *
FROM layoffs_deduped
WHERE percentage_laid_off BETWEEN 25 AND 75
ORDER BY percentage_laid_off DESC;

-- 8️ Outliers: High Funds Raised but Low Layoffs
SELECT company, funds_raised, total_laid_off
FROM layoffs_deduped
WHERE funds_raised > 500 AND total_laid_off < 50
ORDER BY funds_raised DESC;

--  Final Sanity Check
SELECT COUNT(*) AS total_rows_cleaned
FROM layoffs_deduped;

-- =============================
-- FOCUSED VIEW: U.S. or Tech Industry
-- =============================

SELECT * 
FROM layoffs_deduped
WHERE country = 'United States' OR industry = 'Tech';

-- BASIC METRICS (FILTERED)

-- Max layoffs
SELECT MAX(total_laid_off) AS max_layoffs
FROM layoffs_deduped
WHERE country = 'United States' OR industry = 'Tech';

-- Max & Min percentage laid off
SELECT MAX(percentage_laid_off) AS max_pct, MIN(percentage_laid_off) AS min_pct
FROM layoffs_deduped
WHERE (country = 'United States' OR industry = 'Tech')
  AND percentage_laid_off IS NOT NULL;

-- Companies with 100% layoffs
SELECT *
FROM layoffs_deduped
WHERE percentage_laid_off = 100
  AND (country = 'United States' OR industry = 'Tech')
ORDER BY funds_raised DESC;

-- TOTAL LAYOFFS GROUPED

-- Top 10 companies by total layoffs
SELECT company, SUM(total_laid_off) AS total
FROM layoffs_deduped
WHERE country = 'United States' OR industry = 'Tech'
GROUP BY company
ORDER BY total DESC
LIMIT 10;

-- By location
SELECT location, SUM(total_laid_off) AS total
FROM layoffs_deduped
WHERE country = 'United States' OR industry = 'Tech'
GROUP BY location
ORDER BY total DESC
LIMIT 10;

-- By stage
SELECT stage, SUM(total_laid_off) AS total
FROM layoffs_deduped
WHERE country = 'United States' OR industry = 'Tech'
GROUP BY stage
ORDER BY total DESC;

-- By year
SELECT YEAR(date) AS year, SUM(total_laid_off) AS total
FROM layoffs_deduped
WHERE country = 'United States' OR industry = 'Tech'
GROUP BY YEAR(date)
ORDER BY year;

-- By industry
SELECT industry, SUM(total_laid_off) AS total
FROM layoffs_deduped
WHERE country = 'United States' OR industry = 'Tech'
GROUP BY industry
ORDER BY total DESC;

-- LAYOFFS PER $1M RAISED

SELECT 
  company, 
  SUM(total_laid_off) AS total_laid_off,
  SUM(funds_raised) AS total_funds_raised_millions,
  ROUND(SUM(total_laid_off) / NULLIF(SUM(funds_raised), 0), 2) AS layoffs_per_million
FROM layoffs_deduped
WHERE (country = 'United States' OR industry = 'Tech')
  AND funds_raised IS NOT NULL
GROUP BY company
ORDER BY layoffs_per_million DESC
LIMIT 15;

-- ROLLING MONTHLY LAYOFFS (FILTERED)

WITH FilteredMonthly AS (
  SELECT 
    DATE_FORMAT(date, '%Y-%m') AS month,
    SUM(total_laid_off) AS total_laid_off
  FROM layoffs_deduped
  WHERE country = 'United States' OR industry = 'Tech'
  GROUP BY month
)
SELECT 
  month,
  total_laid_off,
  SUM(total_laid_off) OVER (ORDER BY month) AS rolling_total
FROM FilteredMonthly
ORDER BY month;