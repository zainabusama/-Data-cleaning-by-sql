SELECT * FROM world_layoffs.layoffs;
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT INTO layoffs_staging 
SELECT * FROM world_layoffs.layoffs;
SELECT * FROM world_layoffs.layoffs_staging;

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location, industry, total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging
) 
select* 
from duplicate_cte
WHERE  row_num > 1;
#Casper,Cazoo,Hibob,Oda,Terminus,Wildlife Studios,Yahoo

SELECT *
FROM  world_layoffs.layoffs_staging
where company = 'Yahoo';


CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
 row_num INT
);
select *
FROM layoffs_staging2;
INSERT INTO layoffs_staging2
 SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location, industry, total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT distinct industry 
FROM layoffs_staging2
order by 1
;

UPDATE layoffs_staging2
set company = TRIM(company) ;

SELECT distinct industry 
FROM layoffs_staging2
where industry like 'crypto%'
order by 1 ;

update layoffs_staging2
set industry = 'crypto' 
where industry like 'crypto%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

update layoffs_staging2
set country = TRIM(TRAILING '.' FROM country)
where country like 'united states%';



SELECT date
FROM layoffs_staging2;

update layoffs_staging2
set date=STR_TO_DATE(date, '%m/%d/%Y');

alter table layoffs_staging2
modify column date date;




select company ,industry
from layoffs_staging2
where company in('Airbnb','Carvana','Juul')
order by 1
;


select*
from layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company=t2.company
where t1.industry is null or t1.industry = ''
and t2.industry is not null;


update layoffs_staging2
set industry= null
where industry ='';
 
 update layoffs_staging2 t1
 join layoffs_staging2 t2
    on t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is null 
and t2.industry is not null;


select *
from layoffs_staging2;

delete 
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null ;

select *
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null ;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM world_layoffs.layoffs_staging2;








