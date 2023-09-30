import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


#########################################################
#
#   Load Environment Variables
#
#########################################################
# Connection variables
snowflake_conn_id = "snowflake_conn_id"

########################################################
#
#   DAG Settings
#
#########################################################

dag_default_args = {
    'owner': 'assignment3',
    'start_date': datetime.now(),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='assignment3',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Custom Logics for Operator
#
#########################################################

query_refresh_listing = f"""
ALTER EXTERNAL TABLE raw.raw_listing REFRESH;

CREATE OR REPLACE TABLE staging.staging_listing as
SELECT 
    value:c1::int as listing_id
    , value:c2::int as scrape_id
    , value:c3::date as scraped_date
    , value:c4::int as host_id
    , value:c5::varchar as host_name
    , value:c6::string as host_since
    , value:c7::boolean as host_is_superhost
    , value:c8::varchar as host_neighbourhood
    , value:c9::varchar as listing_neighbourhood
    , value:c10::varchar as property_type
    , value:c11::varchar as room_type
    , value:c12::int as accomodates
    , value:c13::int as price
    , value:c14::boolean as has_availability
    , value:c15::int as availability_30
    , value:c16::int as number_of_reviews
    , value:c17::int as review_scores_rating
    , value:c18::int as review_scores_accuracy
    , value:c19::int as review_scores_cleanliness
    , value:c20::int as review_scores_checkin
    , value:c21::int as review_scores_communication
    , value:c22::int as review_scores_value
    , SUBSTRING((split_part(metadata$filename, '/', -1)::varchar),1,2) as mth
    , SUBSTRING((split_part(metadata$filename, '/', -1)::varchar),4,4) as yr
FROM raw.raw_listing;

ALTER TABLE staging.staging_listing ADD year_month date;

UPDATE
  staging.staging_listing
SET
  listing_neighbourhood = UPPER(listing_neighbourhood),
  host_neighbourhood = UPPER(host_neighbourhood),
  year_month = date_from_parts(yr, mth,01);
"""

query_refresh_census = f"""
ALTER EXTERNAL TABLE raw.raw_census_g01 REFRESH;
ALTER EXTERNAL TABLE raw.raw_census_g02 REFRESH;

CREATE OR REPLACE TABLE staging.staging_census_g01 as
SELECT 
    value:c1::varchar as lga_code
    , value:c2::int as tot_p_m
    , value:c3::int as tot_p_f
    , value:c4::int as Age_0_4_yr_M
    , value:c5::int as Age_0_4_yr_F
    , value:c6::int as Age_0_4_yr_P	
    , value:c7::int as Age_5_14_yr_M
    , value:c8::int as Age_5_14_yr_F
    , value:c9::int as Age_5_14_yr_P
    , value:c10::int as Age_15_19_yr_M
    , value:c11::int as Age_15_19_yr_F
    , value:c12::int as Age_15_19_yr_P
    , value:c13::int as Age_20_24_yr_M
    , value:c14::int as Age_20_24_yr_F
    , value:c15::int as Age_20_24_yr_P
    , value:c16::int as Age_25_34_yr_M
    , value:c17::int as Age_25_34_yr_F
    , value:c18::int as Age_25_34_yr_P
    , value:c19::int as Age_35_44_yr_M
    , value:c20::int as Age_35_44_yr_F
    , value:c21::int as Age_35_44_yr_P
    , value:c22::int as Age_45_54_yr_M
    , value:c23::int as Age_45_54_yr_F
    , value:c24::int as Age_45_54_yr_P
    , value:c25::int as Age_55_64_yr_M
    , value:c26::int as Age_55_64_yr_F
    , value:c27::int as Age_55_64_yr_P
    , value:c28::int as Age_65_74_yr_M
    , value:c29::int as Age_65_74_yr_F
    , value:c30::int as Age_65_74_yr_P
    , value:c31::int as Age_75_84_yr_M
    , value:c32::int as Age_75_84_yr_F
    , value:c33::int as Age_75_84_yr_P
    , value:c34::int as Age_85ov_M
    , value:c35::int as Age_85ov_F
    , value:c36::int as Age_85ov_P
FROM raw.raw_census_g01;


CREATE OR REPLACE TABLE staging.staging_census_g02 as
SELECT 
    value:c1::varchar as lga_code
    , value:c2::int as median_age_persons
    , value:c3::int as median_mortage_repay_monthly
    , value:c4::int as median_tot_prsnl_inc_weekly
    , value:c5::int as median_rent_weekly
    , value:c6::int as median_tot_fam_inc_weekly
    , value:c7::int as average_num_psns_per_bedroom
    , value:c8::int as median_tot_hhd_inc_weekly
    , value:c9::int as average_household_size
FROM raw.raw_census_g02;

UPDATE staging.staging_census_g01
SET lga_code = RIGHT(lga_code, LEN(lga_code) - 3)::int;

UPDATE staging.staging_census_g02
SET lga_code = RIGHT(lga_code, LEN(lga_code) - 3)::int;
"""

query_refresh_lga = f"""
ALTER EXTERNAL TABLE raw.raw_lga_code REFRESH;
ALTER EXTERNAL TABLE raw.raw_lga_suburb REFRESH;

CREATE OR REPLACE TABLE staging.staging_lga_code as
SELECT 
    value:c1::int as lga_code
    , value:c2::varchar as lga_name
FROM raw.raw_lga_code;

CREATE OR REPLACE TABLE staging.staging_lga_suburb as
SELECT 
    value:c1::varchar as lga_name
    , value:c2::varchar as suburb_name
FROM raw.raw_lga_suburb;

UPDATE
  staging.staging_lga_code
SET
  lga_name = UPPER(lga_name);
  
UPDATE
  staging.staging_lga_suburb
SET
  lga_name = UPPER(lga_name);
  
CREATE OR REPLACE TABLE staging.staging_lga_joined as 
SELECT s.*, g1.*
    , g2.median_age_persons
    , g2.median_mortage_repay_monthly
    , g2.median_tot_prsnl_inc_weekly
    , g2.median_rent_weekly
    , g2.median_tot_fam_inc_weekly
    , g2.average_num_psns_per_bedroom
    , g2.median_tot_hhd_inc_weekly
    , g2.average_household_size
FROM staging.staging_lga_suburb as s left join staging.staging_lga_code as c 
ON s.lga_name = c.lga_name
inner join staging.staging_census_g01 as g1
ON c.lga_code = g1.lga_code
inner join staging.staging_census_g02 as g2
ON c.lga_code = g2.lga_code
ORDER BY c.lga_code; 
"""

query_create_final_table = f"""
CREATE OR REPLACE TABLE staging.merge1 as
SELECT l.*, c.lga_code as listing_lga
FROM staging.staging_listing as l 
LEFT join staging.staging_lga_code as c
ON l.listing_neighbourhood = c.lga_name;

CREATE OR REPLACE TABLE staging.listing_final as
SELECT l.*, j.lga_code as host_lga
FROM staging.merge1 as l 
LEFT join staging.staging_lga_joined as j
ON l.host_neighbourhood = j.suburb_name;
"""

query_create_datawarehouse = f"""
CREATE OR REPLACE TABLE datawarehouse.fact_listing as
SELECT listing_id, host_id, listing_lga, host_lga, price, has_availability, availability_30, year_month
FROM staging.listing_final;

CREATE OR REPLACE TABLE datawarehouse.dim_host as
SELECT t1.host_id, t1.host_name, t1.host_since, t1.host_is_superhost, t1.host_neighbourhood, t1.host_lga
from staging.listing_final t1
inner join (
    select host_id, max(year_month) as latest
    from staging.listing_final
    group by host_id
) t2 on t1.host_id = t2.host_id and t1.year_month = t2.latest;

CREATE OR REPLACE TABLE datawarehouse.dim_listing as
select t1.listing_id, t1.listing_neighbourhood, t1.property_type, t1.room_type, t1.accomodates, t1.number_of_reviews, t1.review_scores_rating, t1.review_scores_accuracy, t1.review_scores_cleanliness, t1.review_scores_checkin, t1.review_scores_communication, t1.review_scores_value
from staging.listing_final t1
inner join (
    select listing_id, max(year_month) as latest
    from staging.listing_final
    group by listing_id
) t2 on t1.listing_id = t2.listing_id and t1.year_month = t2.latest;

CREATE OR REPLACE TABLE datawarehouse.dim_lga_code as
SELECT *
FROM staging.staging_lga_joined;

CREATE OR REPLACE TABLE datawarehouse.dim_lga_suburb as
SELECT lga_name, suburb_name
FROM staging.staging_lga_suburb;
"""

query_create_datamart = f"""
CREATE OR REPLACE TABLE datamart.dm_listing_neighbourhood as
WITH cte1 as
(
    SELECT listing_neighbourhood, year_month, COUNT(*) as avail_count
    FROM datawarehouse.dim_listing natural join datawarehouse.fact_listing
    WHERE has_availability = TRUE
    GROUP BY listing_neighbourhood, year_month
),

cte2 as
(
    SELECT listing_neighbourhood, year_month, COUNT(*) as total_count
    FROM datawarehouse.dim_listing natural join datawarehouse.fact_listing
    GROUP BY listing_neighbourhood, year_month
),

cte3 as 
(
    SELECT listing_neighbourhood, year_month, MIN(price) as min_price, MAX(price) as max_price, MEDIAN(price) as med_price, AVG(price) as avg_price
    FROM datawarehouse.fact_listing natural join datawarehouse.dim_listing
    WHERE has_availability = TRUE
    GROUP BY listing_neighbourhood, year_month
),

cte4 as 
(
    SELECT listing_neighbourhood, year_month, COUNT(DISTINCT host_id) as distinct_host_id
    FROM datawarehouse.fact_listing natural join datawarehouse.dim_host natural join datawarehouse.dim_listing
    GROUP BY listing_neighbourhood, year_month
),

cte5 as 
(
    SELECT listing_neighbourhood, year_month, COUNT(DISTINCT host_id) as distinct_superhost
    FROM datawarehouse.fact_listing natural join datawarehouse.dim_host natural join datawarehouse.dim_listing
    WHERE host_is_superhost = TRUE 
    GROUP BY listing_neighbourhood, year_month
),

cte6 as
(
    SELECT listing_neighbourhood, year_month, avg(review_scores_rating) as avg_review_score
    FROM datawarehouse.fact_listing natural join datawarehouse.dim_listing
    WHERE has_availability = TRUE 
    GROUP BY listing_neighbourhood, year_month
),

cte7 as
(
    SELECT listing_neighbourhood, year_month, (lag(COUNT(*)) OVER (partition by listing_neighbourhood order by year_month) - 1)  as lag_avail_count
    FROM datawarehouse.dim_listing natural join datawarehouse.fact_listing
    WHERE has_availability = TRUE
    GROUP BY listing_neighbourhood, year_month
),

cte8 as
(
    SELECT listing_neighbourhood, year_month, (lag(COUNT(*)) OVER (partition by listing_neighbourhood order by year_month) - 1)  as lag_total_count
    FROM datawarehouse.dim_listing natural join datawarehouse.fact_listing
    GROUP BY listing_neighbourhood, year_month
),

cte9 as
(
    SELECT listing_neighbourhood, year_month, COUNT(*) as unavail_count
    FROM datawarehouse.dim_listing natural join datawarehouse.fact_listing
    WHERE has_availability = FALSE
    GROUP BY listing_neighbourhood, year_month
),

cte10 as
(
    SELECT listing_neighbourhood, year_month, (lag(COUNT(*)) OVER (partition by listing_neighbourhood order by year_month) - 1)  as lag_unavail_count
    FROM datawarehouse.dim_listing natural join datawarehouse.fact_listing
    WHERE has_availability = FALSE
    GROUP BY listing_neighbourhood, year_month
),

cte11 as
(
    SELECT listing_neighbourhood, year_month, sum(num_stay) as total_num_stay
    FROM (SELECT listing_neighbourhood, year_month, (30-availability_30) num_stay
        FROM datawarehouse.fact_listing natural join datawarehouse.dim_listing
        WHERE has_availability = TRUE)
    GROUP BY listing_neighbourhood, year_month
),

cte12 as
(
    SELECT listing_neighbourhood, year_month, sum(est_price) as total_rev
    FROM (SELECT listing_neighbourhood, year_month, (30-availability_30)*price as est_price
        FROM datawarehouse.fact_listing natural join datawarehouse.dim_listing
        WHERE has_availability = TRUE)
    GROUP BY listing_neighbourhood, year_month
)

SELECT listing_neighbourhood, 
    year_month, 
    (avail_count/total_count)*100 as active_listing_rate, 
    min_price, 
    max_price, 
    med_price, 
    avg_price, 
    distinct_host_id, 
    (distinct_superhost/distinct_host_id)*100 as superhost_rate, 
    avg_review_score, 
    ((avail_count - lag_avail_count)/NULLIF(lag_avail_count,0))*100 as active_pct_change, 
    ((unavail_count - lag_unavail_count)/NULLIF(lag_unavail_count,0))*100 as inactive_pct_change,
    total_num_stay,
    (total_rev/avail_count) as avg_est_price
FROM cte1 natural join cte2 natural join cte3 natural join cte4 natural join cte5 natural join cte6 natural join cte7 natural join cte8 natural join cte9 natural join cte10 natural join cte11 natural join cte12
ORDER BY listing_neighbourhood, year_month;

 
CREATE OR REPLACE TABLE datamart.dm_property_type as
WITH cte1 as
(
    SELECT property_type, room_type, accomodates, year_month, COUNT(*) as avail_count
    FROM datawarehouse.dim_listing natural join datawarehouse.fact_listing
    WHERE has_availability = TRUE
    GROUP BY property_type, room_type, accomodates, year_month
),

cte2 as
(
    SELECT property_type, room_type, accomodates, year_month, COUNT(*) as total_count
    FROM datawarehouse.dim_listing natural join datawarehouse.fact_listing
    GROUP BY property_type, room_type, accomodates, year_month
),

cte3 as 
(
    SELECT property_type, room_type, accomodates, year_month, MIN(price) as min_price, MAX(price) as max_price, MEDIAN(price) as med_price, AVG(price) as avg_price
    FROM datawarehouse.fact_listing natural join datawarehouse.dim_listing
    WHERE has_availability = TRUE
    GROUP BY property_type, room_type, accomodates, year_month
),

cte4 as 
(
    SELECT property_type, room_type, accomodates, year_month, COUNT(DISTINCT host_id) as distinct_host_id
    FROM datawarehouse.fact_listing natural join datawarehouse.dim_host natural join datawarehouse.dim_listing
    GROUP BY property_type, room_type, accomodates, year_month
),

cte5 as 
(
    SELECT property_type, room_type, accomodates, year_month, COUNT(DISTINCT host_id) as distinct_superhost
    FROM datawarehouse.fact_listing natural join datawarehouse.dim_host natural join datawarehouse.dim_listing
    WHERE host_is_superhost = TRUE 
    GROUP BY property_type, room_type, accomodates, year_month
),

cte6 as
(
    SELECT property_type, room_type, accomodates, year_month, avg(review_scores_rating) as avg_review_score
    FROM datawarehouse.fact_listing natural join datawarehouse.dim_listing
    WHERE has_availability = TRUE 
    GROUP BY property_type, room_type, accomodates, year_month
),

cte7 as
(
    SELECT property_type, room_type, accomodates, year_month, (lag(COUNT(*)) OVER (partition by property_type, room_type, accomodates order by year_month) - 1)  as lag_avail_count
    FROM datawarehouse.dim_listing natural join datawarehouse.fact_listing
    WHERE has_availability = TRUE
    GROUP BY property_type, room_type, accomodates, year_month
),

cte8 as
(
    SELECT property_type, room_type, accomodates, year_month, (lag(COUNT(*)) OVER (partition by property_type, room_type, accomodates order by year_month) - 1)  as lag_total_count
    FROM datawarehouse.dim_listing natural join datawarehouse.fact_listing
    GROUP BY property_type, room_type, accomodates, year_month
),

cte9 as
(
    SELECT property_type, room_type, accomodates, year_month, COUNT(*) as unavail_count
    FROM datawarehouse.dim_listing natural join datawarehouse.fact_listing
    WHERE has_availability = FALSE
    GROUP BY property_type, room_type, accomodates, year_month
),

cte10 as
(
    SELECT property_type, room_type, accomodates, year_month, (lag(COUNT(*)) OVER (partition by property_type, room_type, accomodates order by year_month) - 1)  as lag_unavail_count
    FROM datawarehouse.dim_listing natural join datawarehouse.fact_listing
    WHERE has_availability = FALSE
    GROUP BY property_type, room_type, accomodates, year_month
),

cte11 as
(
    SELECT property_type, room_type, accomodates, year_month, sum(num_stay) as total_num_stay
    FROM (SELECT property_type, room_type, accomodates, year_month, (30-availability_30) num_stay
        FROM datawarehouse.fact_listing natural join datawarehouse.dim_listing
        WHERE has_availability = TRUE)
    GROUP BY property_type, room_type, accomodates, year_month
),

cte12 as
(
    SELECT property_type, room_type, accomodates, year_month, sum(est_price) as total_rev
    FROM (SELECT property_type, room_type, accomodates, year_month, (30-availability_30)*price as est_price
        FROM datawarehouse.fact_listing natural join datawarehouse.dim_listing
        WHERE has_availability = TRUE)
    GROUP BY property_type, room_type, accomodates, year_month
)

SELECT property_type, room_type, accomodates, 
    year_month, 
    (avail_count/total_count)*100 as active_listing_rate, 
    min_price, 
    max_price, 
    med_price, 
    avg_price, 
    distinct_host_id, 
    (distinct_superhost/distinct_host_id)*100 as superhost_rate, 
    avg_review_score, 
    ((avail_count - lag_avail_count)/NULLIF(lag_avail_count,0))*100 as active_pct_change, 
    ((unavail_count - lag_unavail_count)/NULLIF(lag_unavail_count,0))*100 as inactive_pct_change,
    total_num_stay,
    (total_rev/avail_count) as avg_est_price
FROM cte1 natural join cte2 natural join cte3 natural join cte4 natural join cte5 natural join cte6 natural join cte7 natural join cte8 natural join cte9 natural join cte10 natural join cte11 natural join cte12
ORDER BY property_type, room_type, accomodates, year_month;


CREATE OR REPLACE TABLE datamart.dm_host_neighbourhood as
WITH cte1 AS 
(
    SELECT f.*, d.lga_name
    FROM datawarehouse.fact_listing as f inner join datawarehouse.dim_lga_code as d
    ON f.host_lga = d.lga_code
),

cte2 as
(
    SELECT f.listing_id, d.lga_name, f.year_month, (30-f.availability_30)*f.price as est_price
    FROM datawarehouse.fact_listing as f inner join datawarehouse.dim_lga_code as d
    ON f.host_lga = d.lga_code
)

SELECT lga_name as host_neighbourhood_lga, 
    year_month, 
    COUNT(DISTINCT host_id) as num_distinct_host,
    sum(est_price) as total_rev,
    (total_rev/num_distinct_host) as avg_rev_per_host
FROM cte1 natural join cte2
GROUP BY host_neighbourhood_lga, year_month
ORDER BY host_neighbourhood_lga, year_month;
"""

#########################################################
#
#   DAG Operator Setup
#
#########################################################

refresh_listing = SnowflakeOperator(
    task_id='refresh_listing_task',
    sql=query_refresh_listing,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

refresh_census = SnowflakeOperator(
    task_id='refresh_census_task',
    sql=query_refresh_census,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

refresh_lga = SnowflakeOperator(
    task_id='refresh_lga_task',
    sql=query_refresh_lga,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

create_final_table = SnowflakeOperator(
    task_id='create_final_table_task',
    sql=query_create_final_table,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

create_datawarehouse = SnowflakeOperator(
    task_id='create_datawarehouse_task',
    sql=query_create_datawarehouse,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

create_datamart = SnowflakeOperator(
    task_id='create_datamart_task',
    sql=query_create_datamart,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

refresh_listing >> refresh_census  >> refresh_lga >> create_final_table >> create_datawarehouse >> create_datamart