------------ Part 3
-- Q1 What are the main differences from a population point of view (i.g. higher population of under 30s) between the best performing “listing_neighbourhood” and the worst (in terms of estimated revenue per active listings) over the last 12 months? 

-- Best perform = northern beaches
SELECT listing_neighbourhood, sum(est_price) as total_rev
FROM (SELECT listing_neighbourhood, year_month, (30-availability_30)*price as est_price
    FROM datawarehouse.fact_listing natural join datawarehouse.dim_listing
    WHERE has_availability = TRUE)
GROUP BY listing_neighbourhood
ORDER BY total_rev desc
LIMIT 1;

-- Worst perform = Camden
SELECT listing_neighbourhood, sum(est_price) as total_rev
FROM (SELECT listing_neighbourhood, year_month, (30-availability_30)*price as est_price
    FROM datawarehouse.fact_listing natural join datawarehouse.dim_listing
    WHERE has_availability = TRUE)
GROUP BY listing_neighbourhood
ORDER BY total_rev
LIMIT 1;

-- 6074
SELECT d.lga_name, d.Age_20_24_yr_P
FROM datawarehouse.fact_listing as f inner join datawarehouse.dim_lga_code as d
ON f.listing_lga = d.lga_code
WHERE f.listing_lga = (SELECT distinct lga_code FROM datawarehouse.dim_lga_code WHERE lga_name = 'NORTHERN BEACHES')
LIMIT 1;

-- 2425
SELECT d.lga_name, d.Age_20_24_yr_P
FROM datawarehouse.fact_listing as f inner join datawarehouse.dim_lga_code as d
ON f.listing_lga = d.lga_code
WHERE f.listing_lga = (SELECT distinct lga_code FROM datawarehouse.dim_lga_code WHERE lga_name = 'CAMDEN')
LIMIT 1;

-- Q2 What will be the best type of listing (property type, room type and accommodates for) for the top 5 “listing_neighbourhood” (in terms of estimated revenue per active listing) to have the highest number of stays?

CREATE OR REPLACE TEMPORARY VIEW temp AS
WITH cte1 as
(
    SELECT listing_neighbourhood, COUNT(*) as avail_count
    FROM datawarehouse.dim_listing natural join datawarehouse.fact_listing
    WHERE has_availability = TRUE
    GROUP BY listing_neighbourhood
),

cte2 as 
(
    SELECT listing_neighbourhood, sum(est_price) as total_rev
    FROM (SELECT listing_neighbourhood, (30-availability_30)*price as est_price
        FROM datawarehouse.fact_listing natural join datawarehouse.dim_listing
        WHERE has_availability = TRUE)
    GROUP BY listing_neighbourhood
)
SELECT listing_neighbourhood, (total_rev/avail_count) as avg_est_price
FROM cte1 natural join cte2
ORDER BY avg_est_price DESC
LIMIT 5;

CREATE OR REPLACE TEMPORARY VIEW temp1 AS
SELECT distinct listing_neighbourhood, property_type, room_type, accomodates
FROM datawarehouse.fact_listing NATURAL JOIN datawarehouse.dim_listing
WHERE listing_neighbourhood IN (select distinct listing_neighbourhood from temp);

WITH cte1 AS 
(SELECT listing_neighbourhood, property_type
 FROM (
SELECT listing_neighbourhood, property_type
  , ROW_NUMBER() OVER (PARTITION BY listing_neighbourhood ORDER BY COUNT(property_type) DESC) rn
FROM temp1
GROUP BY
    listing_neighbourhood 
  , property_type)
 WHERE rn = 1),
 
cte2 as 
 (SELECT listing_neighbourhood, room_type
 FROM (
SELECT listing_neighbourhood, room_type
  , ROW_NUMBER() OVER (PARTITION BY listing_neighbourhood ORDER BY COUNT(room_type) DESC) rn
FROM temp1
GROUP BY
    listing_neighbourhood 
  , room_type)
 WHERE rn = 1),
 
cte3 as 
 (SELECT listing_neighbourhood, accomodates
 FROM (
SELECT listing_neighbourhood, accomodates
  , ROW_NUMBER() OVER (PARTITION BY listing_neighbourhood ORDER BY COUNT(accomodates) DESC) rn
FROM temp1
GROUP BY
    listing_neighbourhood 
  , accomodates)
 WHERE rn = 1)
 
 SELECT *
 FROM cte1 natural join cte2 natural join cte3;

-- Q3 Do hosts with multiple listings are more inclined to have their listings in the same LGA as where they live?
WITH cte AS
(
SELECT DISTINCT host_id
FROM datawarehouse.fact_listing 
WHERE listing_lga = host_lga
GROUP BY host_id
HAVING COUNT(distinct listing_lga) > 1
ORDER BY HOST_ID)

SELECT COUNT(*) as count_same_LGA
FROM cte;

WITH cte AS
(
SELECT DISTINCT host_id
FROM datawarehouse.fact_listing 
WHERE listing_lga != host_lga
GROUP BY host_id
HAVING COUNT(distinct listing_lga) > 1
ORDER BY HOST_ID)

SELECT COUNT(*) as _count_diff_LGA
FROM cte;

-- Q4 For hosts with a unique listing, does their estimated revenue over the last 12 months can cover the annualised median mortgage repayment of their listing’s “listing_neighbourhood”?
CREATE OR REPLACE TEMPORARY VIEW temp2 AS 
SELECT * FROM (
WITH cte1 AS
(
    SELECT DISTINCT host_id
    FROM datawarehouse.fact_listing
    GROUP BY host_id
    HAVING COUNT(listing_id) > 1
),

cte2 as
(
    SELECT DISTINCT f.host_id, f.host_lga, d.median_mortage_repay_monthly
    FROM datawarehouse.fact_listing as f inner join datawarehouse.dim_lga_code as d
    ON f.listing_lga = d.lga_code
),

cte3 as
(
    SELECT host_id, sum(est_price) as total_rev
    FROM (SELECT host_id, (30-availability_30)*price as est_price
        FROM datawarehouse.fact_listing natural join datawarehouse.dim_listing
        WHERE has_availability = TRUE)
    GROUP BY host_id
)

SELECT DISTINCT host_id, total_rev, median_mortage_repay_monthly*12 as yearly_mortgage
FROM cte1 natural join cte2 natural join cte3 )
;

SELECT count(host_id) as can_afford
FROM temp2
WHERE total_rev >= yearly_mortgage;

SELECT count(host_id) as cannot_afford
FROM temp2
WHERE total_rev < yearly_mortgage;
