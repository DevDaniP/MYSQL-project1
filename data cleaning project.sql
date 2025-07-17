-- data cleaning 
SELECT * 
FROM layoffs; 
-- 1. remove duplicates 
-- finding the duplicates 
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- checking all the info that is listed more than once 
WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1; 

-- in order to now delete these duplicates we need to add only those to a staging area 2 
-- basically were creating another table that actual has that row_number row in the table 
CREATE TABLE layoffs_staging2 (
	company text,
	location text, 
	industry text,
	total_laid_off int, 
	percentage_laid_off text, 
	date text, 
	stage text, 
	country text, 
	funds_raised_millions int,
    row_num INT 
);
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_num
FROM layoffs_staging; 

-- finally deleting the duplicates 
-- extra work around i did because it wouldnt let me 
SET SQL_SAFE_UPDATES = 0;
DELETE 
FROM layoffs_staging2
WHERE row_num > 1; 
SET SQL_SAFE_UPDATES = 1;

SELECT * 
FROM layoffs_staging2;
-- 2. standarize the data 
-- starting with removing leading and trailing whitespace 
SELECT company, TRIM(company)
FROM layoffs_staging2; 

UPDATE layoffs_staging2
SET company = TRIM(company); 

-- combining rows that are the same like crypto and cryptocurrency 
SELECT * 
FROM layoffs_staging2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_staging2
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';

SELECT DISTINCT industry 
FROM layoffs_staging2; 

-- looking through country 
-- one of the united states had a period at the end 
SELECT country 
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1; 

-- trick to see where the exact places the periods are located
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1; 

UPDATE layoffs_staging2
SET country =  TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'; 
-- checking if it worked 
SELECT country 
FROM layoffs_staging2
WHERE country LIKE 'United States.%';

-- fixing the way the date data is interpreted
SELECT `date`, 
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2; 

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE; 

-- 3. null values or blank values 
-- working with null or blank values 
-- these are specifically the ones that are null for both 
-- which means teh info is prolly useless to us 
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; 

SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- finding the nulls or blanks within industry 
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';  

-- looking closer at the ones that query showed us 
-- we look at the other listings for the company and can then 
-- determine the missing industry values 
SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb'; 

-- this shows us listings that are blank in some places and filled out in others 
SELECT * 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company = t2.company
  AND t1.location = t1.location
WHERE (t1.industry IS NULL OR t1.industry = '') 
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '') 
AND t2.industry IS NOT NULL;

-- after doing that last one it didnt seem to work. 
-- so the next thing to try is to change the blanks into nulls then change the nulls 
UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';

-- now lets try it again but get rid of the blanks in the query 
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- after that was done we only saw one industry that was left as a null 
SELECT * 
FROM layoffs_staging2 
WHERE company LIKE 'Bally%'; 

-- thats the end of the null values because the rest cannot be inferred by something else in the table 

-- 4. remove any columns or rows uneeded 

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; 

-- were getting rid of the rows with no useful info in stats
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- get rid of the ruw_num we created when working with duplicates 
ALTER TABLE layoffs_staging2
	DROP COLUMN row_num;

SELECT * 
FROM layoffs_staging2;




