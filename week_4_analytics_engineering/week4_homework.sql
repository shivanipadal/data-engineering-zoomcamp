-- week4 
-- Question 1:
-- What is the count of records in the model fact_trips after running all models with the test run variable disabled and 
-- filtering for 2019 and 2020 data only (pickup datetime)?

SELECT count(*) FROM `ny-rides-shivani.production.fact_trips` a where EXTRACT(YEAR FROM pickup_datetime) in (2019, 2020) 
-- 61612953


-- Question 2:
-- What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos?
 SELECT 
 service_type, count(*) FROM `ny-rides-shivani.production.fact_trips` a 
 where EXTRACT(YEAR FROM pickup_datetime) in (2019, 2020)  group by service_type

-- Green
-- 6254642	
-- Yellow
-- 55358311
-- 
-- 89.9/10.1

--Question 3:
--What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)?

SELECT count(*) FROM `ny-rides-shivani.production.stg_fhv_tripdata` where EXTRACT(YEAR from pickup_datetime) = 2019
-- 43244696


--Question 4:
--What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?
SELECT count(1) FROM `ny-rides-shivani.production.fact_fhv_trips` where EXTRACT(YEAR from pickup_datetime) = 2019
-- 22998722

-- Question 5:
-- What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table?

SELECT EXTRACT(MONTH from pickup_datetime) , count(1) FROM `ny-rides-shivani.production.fact_fhv_trips` where EXTRACT(YEAR from pickup_datetime) = 2019 group by EXTRACT(MONTH from pickup_datetime) order by 2 desc

-- January
