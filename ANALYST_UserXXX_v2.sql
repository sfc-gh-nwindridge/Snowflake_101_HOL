-----------------------------------------------------------------------------------
-- Replace 'XXX' with the number from your userid
-----------------------------------------------------------------------------------

-----------------------------------------------------------------------------------
-----ANALYST PART I
-----------------------------------------------------------------------------------

--1.1.1 Set your context to use your ANALYST role
use role analystXXX;
use warehouse query_wh;
use schema citibike.schemaXXX;


--1.1.2 How many trips per hour? How long/how far do people ride?
-- *** We should get an error here due to the extra comma at the end of the select statement, let's see what happens ***
select date_trunc('hour', starttime) as "date",
       count(*) as "num trips",
       avg(tripduration)/60 as "avg duration (mins)",
       avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)",
from trips
group by 1 
order by 1;
-- How long did the query take? Let's look at the query plan

-- Let's look at the Result Cache 
-- re-run the query from 1.1.2 - How did it take? Let's look at the query plan


--1.1.3 Which day of the week is the busiest1
select dayname(starttime) as "day of week",
       count(*) as "num trips"
from trips
group by 1 
order by 2 desc;

----------------------------------------------------------------------------------------
----- BACK TO DBA WORKSHEET
----------------------------------------------------------------------------------------



-----------------------------------------------------------------------------------
-----ANALYST PART II
-----------------------------------------------------------------------------------
--1.2.1 What were the busiest 20 stations and the average temperature recorded1
select start_station_name as "station",
       count(*) as "num rides",
       round(avg(temp_avg),2) as "avg temp"
from trip_weather_vw
group by 1 
order by 2 desc
limit 20;

----------------------------------------------------------------------------------------
----- BACK TO DBA WORKSHEET
----------------------------------------------------------------------------------------
