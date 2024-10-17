-----------------------------------------------------------------------------------
-- Replace 'XXX' with the number from your userid
-----------------------------------------------------------------------------------

----------------------------------------------------------------------------------------
-----DBA PART I
----------------------------------------------------------------------------------------

--1.1.1 Set your context to use your DBA role and assigned SCHEMA
use role dbaXXX;
use schema citibike.schemaXXX;


--1.1.2 Create a table called trips to hold our Citibike data
create or replace table trips
(tripduration integer,
  starttime timestamp,
  stoptime timestamp,
  start_station_id integer,
  start_station_name string,
  start_station_latitude float,
  start_station_longitude float,
  end_station_id integer,
  end_station_name string,
  end_station_latitude float,
  end_station_longitude float,
  bikeid integer,
  membership_type string,
  usertype string,
  birth_year integer,
  gender integer);

--1.1.3 Create the stage
create or replace stage trip_data_s3
  url = 's3://snowflake-workshop-lab/citibike-trips-csv/';

--1.1.4 Have a look at the staged data
ls @trip_data_s3;

--1.1.5 Create a file format that matches our CSV data (OR RUN VIA UI)
create or replace file format csv
  type='csv'
  compression = 'auto' 
  field_delimiter = ',' 
  record_delimiter = '\n'
  skip_header = 0 
  field_optionally_enclosed_by = '\042' 
  trim_space = false
  error_on_column_count_mismatch = false 
  escape = 'none' 
  escape_unenclosed_field = '\134'
  date_format = 'auto' 
  timestamp_format = 'auto' null_if = ('');

-- Example File formats:
-- CREATE OR REPLACE FILE FORMAT BUK_SNOWCAMP.public.json_ff type = 'json';


--1.1.6 Create a Warehouse to load our data (OR RUN VIA UI)
create or replace warehouse load_whXXX
with warehouse_size = 'medium'
auto_suspend = 300
auto_resume = true
min_cluster_count = 1
max_cluster_count = 1
scaling_policy = 'standard';


--1.1.7 Adjust your context to use your new warehouse
use warehouse load_whXXX;
use database citibike;
use schema schemaXXX;


--1.1.8 Load some of the data!
copy into trips
from @trip_data_s3/
pattern='.*2018.*csv[.]gz'
file_format=csv;


--1.1.9 Let’s scale our compute UP by increasing our Warehouse size to X-Large:
alter warehouse load_whXXX set warehouse_size='xlarge';

--1.1.10 Then load the rest of the data:
copy into trips
from @trip_data_s3/
file_format=csv;
--pattern='.*csv[.]gz';


--1.1.11 Now that the data is loaded, we can run simple SQL
--Check the number of rows loaded
select count(*) from trips;


-- And a sample of the data
select * from trips limit 20;


--1.1.12 Grant your ANALYST user access to the data
grant usage on database citibike to role analystXXX;
grant usage on schema citibike.schemaXXX to role analystXXX;
grant select on table citibike.schemaXXX.trips to role analystXXX;

----------------------------------------------------------------------------------------
----- GO TO ANALYST WORKSHEET
----------------------------------------------------------------------------------------



----------------------------------------------------------------------------------------
-----DBA PART II
----------------------------------------------------------------------------------------

--1.2.1 CLONE THE SCHEMA - Create a dev schema by cloning schema
create schema schemaXXX_dev clone schemaXXX;

--1.2.2 QUERY JSON
select *
from WEATHER_DATA.PUBLIC.HOL_WEATHER
limit 20;

--1.2.3 Check out how we can query JSON data with SQL, as if it were a structured table!
select
  dateadd('year',-2,v:time::timestamp) as observation_time,
  v:city.id::int as city_id,
  v:city.name::string as city_name,
  v:city.country::string as country,
  v:clouds.all::int as clouds,
  (v:main.temp::float)-273.15 as temp_avg,
  (v:main.temp_min::float)-273.15 as temp_min,
  (v:main.temp_max::float)-273.15 as temp_max,
  v:wind.deg::float as wind_dir,
  v:wind.speed::float as wind_speed
from WEATHER_DATA.PUBLIC.HOL_WEATHER
where city_id = 5128638 limit 20;

-- 1.2.4 - Challenge 1
-- Alter the above query to include city latitude and longitude as city_lat and city_long
-- https://docs.snowflake.com/en/user-guide/querying-semistructured#dot-notation

-- Also include the weather data. Hint, the weather data may contain repeated elements, take the 1st element
-- https://docs.snowflake.com/en/user-guide/querying-semistructured#retrieving-a-single-instance-of-a-repeating-element

-- What if you try to extract too many elements from a repeated elements field?





















--1.2.5: Challenge 1 solution
select
  dateadd('year',-2,v:time::timestamp) as observation_time,
  v:city.id::int as city_id,
  v:city.name::string as city_name,
  v:city.country::string as country,
  v:city.coord.lat::float as city_lat,
  v:city.coord.lon::float as city_lon,
  v:clouds.all::int as clouds,
  (v:main.temp::float)-273.15 as temp_avg,
  (v:main.temp_min::float)-273.15 as temp_min,
  (v:main.temp_max::float)-273.15 as temp_max,
  v:weather[0].main::string as weather,
  v:weather[0].description::string as weather_desc,
  v:weather[0].icon::string as weather_icon,
  v:weather[1].main::string as weather2,
  v:wind.deg::float as wind_dir,
  v:wind.speed::float as wind_speed
from WEATHER_DATA.PUBLIC.HOL_WEATHER
where city_id = 5128638 limit 20;


--1.2.6 Let's first create a view in schema1_dev
create or replace view citibike.schemaXXX_dev.weather_vw as
select
  dateadd('year',-2,v:time::timestamp) as observation_time,
  v:city.id::int as city_id,
  v:city.name::string as city_name,
  v:city.country::string as country,
  v:city.coord.lat::float as city_lat,
  v:city.coord.lon::float as city_lon,
  v:clouds.all::int as clouds,
  (v:main.temp::float)-273.15 as temp_avg,
  (v:main.temp_min::float)-273.15 as temp_min,
  (v:main.temp_max::float)-273.15 as temp_max,
  v:weather[0].main::string as weather,
  v:weather[0].description::string as weather_desc,
  v:weather[0].icon::string as weather_icon,
  v:wind.deg::float as wind_dir,
  v:wind.speed::float as wind_speed
from WEATHER_DATA.PUBLIC.HOL_WEATHER
where city_id = 5128638;


--1.2.7 Verify the data before pushing to PROD schema
select * from citibike.schemaXXX_dev.weather_vw limit 20;


--1.2.8 All good, create the view in PROD!
create or replace view citibike.schemaXXX.weather_vw as
select
  dateadd('year',-2,v:time::timestamp) as observation_time,
  v:city.id::int as city_id,
  v:city.name::string as city_name,
  v:city.country::string as country,
  v:city.coord.lat::float as city_lat,
  v:city.coord.lon::float as city_lon,
  v:clouds.all::int as clouds,
  (v:main.temp::float)-273.15 as temp_avg,
  (v:main.temp_min::float)-273.15 as temp_min,
  (v:main.temp_max::float)-273.15 as temp_max,
  v:weather[0].main::string as weather,
  v:weather[0].description::string as weather_desc,
  v:weather[0].icon::string as weather_icon,
  v:wind.deg::float as wind_dir,
  v:wind.speed::float as wind_speed
from WEATHER_DATA.PUBLIC.HOL_WEATHER
where city_id = 5128638;

--1.2.9 Drop the DEV schema
drop schema schemaXXX_dev;
use schema schemaXXX;


-------------------------------------------------------------------------------
--1.2.10 COMBINED VIEW - Now that we have the weather_vw, let's combine it with our trips table
create or replace view trip_weather_vw as
select * 
from trips
left outer join weather_vw
on date_trunc('hour', observation_time) = date_trunc('hour', starttime);


--1.2.11 Now we can see what the weather was like at the start of a ride!
select weather as conditions,
       count(*) as "num trips"
from trip_weather_vw
where conditions is not null
group by 1 order by 2 desc;

--1.2.12 Just like before, let's give our ANALYST access to this new view.
grant select on view citibike.schemaXXX.trip_weather_vw to role analystXXX;

----------------------------------------------------------------------------------------
----- GO TO ANALYST WORKSHEET
----------------------------------------------------------------------------------------



-------------------------------------------------------------------------------
--1.3.1 TIME TRAVEL - Accidentally drop the trips table
drop table trips;
select * from trips limit 10;

--1.3.2 Restore with an UNdrop!
undrop table trips;
select * from trips limit 10;


-------------------------------------------------------------------------------
--1.3.3 ROLL BACK A TABLE - Accidentally mess up the data and replace all the station names with "oops"
update trips set start_station_name = 'oops';


--1.3.4 Try to list the top 20 stations...
select start_station_name as "station",
       count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;


--1.3.5 Fix it by finding the query_id and then rolling the table back
set query_id =
(
 select query_id 
 from table(information_schema.query_history_by_session (result_limit=>5))
 where query_text like 'update%' order by start_time limit 1
);

create or replace table trips as
    (select * from trips before (statement => $query_id));

--Run the query in 4.2.2 again and ta da! The data is fixed :)


--1.3.6 Challenge 2
-- Let's use timestamps and offset to re-create the trips table as at 5 minutes ago, hint there are 2 ways
-- https://docs.snowflake.com/en/user-guide/data-time-travel#querying-historical-data

-- Tip: Timestamp will vary depending on your location, use the below query to get the current timestamp for your location
select current_timestamp();






















--1.3.7 Challenge 2 Solution
create or replace table trips as
    (select * from trips AT(TIMESTAMP => 'Thu, 17 Oct 2024 14:30:00 -0700'::timestamp_tz));

create or replace table trips as
    (select * from trips AT(OFFSET => -60*5));





-------------------------------------------------------------------------------
--1.4.1 CREATE AN OUTBOUND SHARE - Create a share called Citibike
create or replace share citibikeXXX;

--1.4.2 Grant usage of the citibike database & schema to the share
grant usage on database citibike to share citibikeXXX;
grant usage on schema citibike.schemaXXX to share citibikeXXX;
grant select on all tables in schema citibike.schemaXXX to share citibikeXXX;

--1.4.3 Add the pre-created Reader account (MB12906) to the share
alter share citibikeXXX add account=MB12906;


-------------------------------------------------------------------------------
--1.5.1 UPDATE THE SHARE - Let’s see how many rides per Membership type – notice there are a lot of NULLs:
select membership_type, count(*) from trips
group by 1 order by 2 desc;

--1.5.2 Let’s remove the NULLs from the table
delete from trips where membership_type is null;