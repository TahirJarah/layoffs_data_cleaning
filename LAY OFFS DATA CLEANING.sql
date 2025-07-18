use world_layoffs;

-- REMOVING DUPLICATES
create table layoffs_data 
like layoffs;

select*
from layoffs_data;

insert into layoffs_data
select*
from layoffs;

with removing_duplicates as (
SELECT*,
row_number() over (partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) row_num
from layoffs_data
)
select*
from removing_duplicates;

create table layoff_data2
 (company text, location text, industry text, 
 total_laid_off int default null, percentage_laid_off text, date text, 
 stage text, country text, funds_raised_millions int default null,
 row_numb int );
 
 insert into layoff_data2
select *,
row_number() over (
partition by company, location, industry,  total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) row_numb
from layoffs_staging;
 
 select* 
 from layoff_data2
 where row_numb > 1;


delete
from layoff_data2
where row_numb > 1;

select*
from layoff_data2
where row_numb > 1;

-- standardization (spaces, spellings)
-- white spaces

select*
from layoff_data2;

-- TRIM
select distinct company
from layoff_data2;

select company, trim(company)
from layoff_data2;

update layoff_data2
set company = trim(company);

select distinct location
from layoff_data2
order by 1;

select location, trim(location)
from layoff_data2;

update layoff_data2
set location = trim(location);

select distinct industry
from layoff_data2
order by 1;

select industry, trim(industry)
from layoff_data2;

update layoff_data2
set industry = trim(industry);

select *
from layoff_data2;
where industry like 'Crypto currency%';

update layoff_data2
set industry = 'Crypto'
where industry like 'Crypto currency%';

select distinct country
from layoff_data2
order by 1;

select distinct country , trim(trailing ' , ' from country)
from layoff_data2
where country like 'United States%';

update layoff_data2
set country = trim(trailing ' , ' from country)
where country like 'United States%';

select *
from layoff_data2;

-- NULL values AND BLANK SPACES
select *
from layoff_data2
where total_laid_off is null
or percentage_laid_off = ' ';

select *
from layoff_data2
where total_laid_off is null
or percentage_laid_off = ' ';


-- to fill the data in the blank area
select t1.industry, t2.industry
from layoff_data2 t1
join layoff_data2 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoff_data2
set industry = null
where industry = '';

update layoff_data2 t1
join  layoff_data2 t2
	on t1.company = t2.company
set  t1.industry = t2.industry
where  t1.industry is null 
and t2.industry is not null ;

select *
from layoff_data2
where company = 'Airbnb';


select *
from layoff_data2
WHERE total_laid_off is null and 
	percentage_laid_off is null;
    
select *
from layoff_data2; 

-- Deleting Column
alter table layoff_data2
drop column row_numb;
    
delete 
from layoff_data2
WHERE total_laid_off is null and 
	percentage_laid_off is null;

select *
from layoff_data2
WHERE company like 'Ball%';

-- date
-- STANDARDIZING DATE FROM M/D/Y TO Y-M-d

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoff_data2;

update layoff_data2
set date = str_to_date(`date`, '%m/%d/%Y');
