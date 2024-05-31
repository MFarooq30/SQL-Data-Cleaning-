# SQL-Data-Cleaning for a data set consisting information about laid off employee during COVID 

-- Remove duplicate records based on certain criteria
WITH Duplicate_CTE AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY company, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS r_num
    FROM layoffs_Staging
) 
DELETE FROM Duplicate_CTE
WHERE r_num > 1;

-- Create a new table for staging layoffs data
CREATE TABLE layoffs_staging2 (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT DEFAULT NULL,
    percentage_laid_off TEXT,
    date TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Populate the new table with cleaned data
INSERT INTO layoffs_staging2
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY company, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS r_num
FROM layoffs_Staging;

-- Retrieve distinct industry values from the staging table
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- Remove trailing dots from the country names
SELECT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2;

-- Update country names to remove trailing dots specifically for 'United States'
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Convert the date strings to proper date format
SELECT date, STR_TO_DATE(date, '%m/%d/%YYYY')
FROM layoffs_staging2;

-- Update missing industry values based on company matches
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

-- Set industry values to NULL where they are empty
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Remove records with missing total_laid_off and percentage_laid_off values
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Remove the Row_Num column from the table
ALTER TABLE layoffs_staging2
DROP COLUMN Row_Num;

-- Retrieve the maximum total laid off and the maximum percentage laid off from 'layoffs_staging2' table
SELECT 
    MAX(total_laid_off) AS max_total_laid_off, 
    MAX(percentage_laid_off) AS max_percentage_laid_off
FROM layoffs_staging2;

-- Retrieve company names and the sum of total laid off employees, grouped by company and ordered by the sum in descending order
SELECT 
    company, 
    SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
GROUP BY company
ORDER BY total_laid_off_sum DESC;

-- Retrieve the year and the sum of total laid off employees, grouped by year and ordered by the sum in descending order
SELECT 
    YEAR(date) AS year, 
    SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
GROUP BY YEAR(date)
ORDER BY total_laid_off_sum DESC;

-- Calculate rolling total of lost jobs per month
WITH Rolling_total AS (
    SELECT 
        SUBSTRING(DATE, 1, 7) AS Month, 
        SUM(total_laid_off) AS Lost_Jobs
    FROM layoffs_staging2
    WHERE SUBSTRING(DATE, 1, 7) IS NOT NULL
    GROUP BY Month
    ORDER BY Lost_Jobs
)
SELECT 
    Month, 
    Lost_Jobs,
    SUM(Lost_Jobs) OVER (ORDER BY Month) AS Rolling_Total
FROM Rolling_total;

-- Retrieve company names, year, and the sum of total laid off employees, grouped by company and year, ordered by company in descending order
SELECT 
    company, 
    YEAR(date) AS year, 
    SUM(total_laid_off) AS total_laid_off_sum
FROM layoffs_staging2
WHERE SUM(total_laid_off) IS NOT NULL
GROUP BY company, YEAR(date)
ORDER BY company DESC;

-- Retrieve top 5 companies with the highest total laid off employees for each year
WITH Company_Year AS (
    SELECT 
        company, 
        YEAR(date) AS years, 
        SUM(total_laid_off) AS total_laid_off
    FROM layoffs_staging2
    GROUP BY company, YEAR(date)
), Company_year_Rank AS (
    SELECT 
        *,
        DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
    FROM Company_Year
    WHERE years IS NOT NULL
)
SELECT * 
FROM Company_year_Rank
WHERE Ranking <= 5;
