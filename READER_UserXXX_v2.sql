-----------------------------------------------------------------------------------
-- Replace 'XXX' with the number from your userid
-----------------------------------------------------------------------------------

-------------------------------------------------------------------------------
--1.1.1 IMPORT THE SHARE - Import shared data from provider account
-- Data Products -> Private Sharing
-- Name the database citibikeXXX

use schema citibikeXXX.schemaXXX;

--1.1.2 Count the number of rows and see a sample
select count (*) from trips;


-------------------------------------------------------------------------------
--***GO BACK TO THE PROVIDER ACCOUNT TO LOAD THE REST OF THE DATA***
-------------------------------------------------------------------------------

--1.2.1	Count the number of rows again - the NULLS should be gone (should be ~7m rows now):
select count (*) from citibikeXXX.schemaXXX.trips;