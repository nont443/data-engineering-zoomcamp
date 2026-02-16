-- Question 3

select count(1) from `zoomcamp-486023.zoomcamp.fct_monthly_zone_revenue`;

-- Question 4

SELECT pickup_zone,SUM(revenue_monthly_total_amount) FROM `zoomcamp-486023.zoomcamp.fct_monthly_zone_revenue` 
where service_type='Green'  and EXTRACT(YEAR FROM revenue_month) = 2020
group by 1 order by 2 desc;

-- Question 5

SELECT sum(total_monthly_trips) FROM `zoomcamp-486023.zoomcamp.fct_monthly_zone_revenue` 
where service_type='Green'  and EXTRACT(YEAR FROM revenue_month) = 2019 and EXTRACT(MONTH FROM revenue_month) = 10;


-- Question 6

SELECT count(1) FROM `zoomcamp-486023.zoomcamp.fhv_tripdata` where dispatching_base_num IS NOT NULL ;