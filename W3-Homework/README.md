# DE Zoomcamp Week 3 Homework

# Create External Table by using table wildcard in the UI , to get all parquet file into single table
# gs://zoomcamp-486023-homework3/yellow_tripdata_2024-*.parquet

# Create native table , not partition or cluster
CREATE OR REPLACE TABLE `zoomcamp-486023.zoomcamp.yellow_tripdata_2024_native` 
AS
SELECT * FROM `zoomcamp-486023.zoomcamp.yellow_tripdata_2024` ;

# Homework 3 Question 1
# use preview table to see # of row
# 20332093

# Homework 3 Question 2
# External Table = 0 B
# Native Table = 155.12 MB

select count(PULocationID) 
from `zoomcamp.yellow_tripdata_2024`;

select count(PULocationID) 
from `zoomcamp.yellow_tripdata_2024_native`;


# Homework 3 Question 3
# PULocationID = 155.12 MB
# PULocationID and DOLocationID = 310.24 MB

select PULocationID
from `zoomcamp.yellow_tripdata_2024_native`;

select PULocationID, DOLocationID
from `zoomcamp.yellow_tripdata_2024_native`;

# Homework 3 Question 4
# record of fare_amount = 0  is 8333

select count(1)
from `zoomcamp.yellow_tripdata_2024_native`
where fare_amount = 0;

# Homework 3 Question 5
# Create native table ,  partition by tpep_dropoff_datetime and cluster by VendorID

CREATE OR REPLACE TABLE `zoomcamp-486023.zoomcamp.yellow_tripdata_2024_partition_cluster` 
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `zoomcamp-486023.zoomcamp.yellow_tripdata_2024_native`;

# Homework 3 Question 6
# non-partition table = 310.24 MB
# partition table = 26.84 MB

select distinct(VendorID)
from `zoomcamp-486023.zoomcamp.yellow_tripdata_2024_native`
where DATE(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15';

select distinct(VendorID)
from `zoomcamp-486023.zoomcamp.yellow_tripdata_2024_partition_cluster`
where DATE(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15';

# Homework 3 Question 9
# count(*) read 0 B because it has information from table metadata or information_schema already

select count(*)
from `zoomcamp-486023.zoomcamp.yellow_tripdata_2024_partition_cluster`;

# Create External Table by using table wildcard in the UI , to get all parquet file into single table
# gs://zoomcamp-486023-homework3/yellow_tripdata_2024-*.parquet

# Create native table , not partition or cluster
CREATE OR REPLACE TABLE `zoomcamp-486023.zoomcamp.yellow_tripdata_2024_native` 
AS
SELECT * FROM `zoomcamp-486023.zoomcamp.yellow_tripdata_2024` ;

# Homework 3 Question 1
# use preview table to see # of row
# 20332093

# Homework 3 Question 2
# External Table = 0 B
# Native Table = 155.12 MB

select count(PULocationID) 
from `zoomcamp.yellow_tripdata_2024`;

select count(PULocationID) 
from `zoomcamp.yellow_tripdata_2024_native`;


# Homework 3 Question 3
# PULocationID = 155.12 MB
# PULocationID and DOLocationID = 310.24 MB

select PULocationID
from `zoomcamp.yellow_tripdata_2024_native`;

select PULocationID, DOLocationID
from `zoomcamp.yellow_tripdata_2024_native`;

# Homework 3 Question 4
# record of fare_amount = 0  is 8333

select count(1)
from `zoomcamp.yellow_tripdata_2024_native`
where fare_amount = 0;

# Homework 3 Question 5
# Create native table ,  partition by tpep_dropoff_datetime and cluster by VendorID

CREATE OR REPLACE TABLE `zoomcamp-486023.zoomcamp.yellow_tripdata_2024_partition_cluster` 
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `zoomcamp-486023.zoomcamp.yellow_tripdata_2024_native`;

# Homework 3 Question 6
# non-partition table = 310.24 MB
# partition table = 26.84 MB

select distinct(VendorID)
from `zoomcamp-486023.zoomcamp.yellow_tripdata_2024_native`
where DATE(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15';

select distinct(VendorID)
from `zoomcamp-486023.zoomcamp.yellow_tripdata_2024_partition_cluster`
where DATE(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15';

# Homework 3 Question 9
# count(*) read 0 B because it has information from table metadata or information_schema already

select count(*)
from `zoomcamp-486023.zoomcamp.yellow_tripdata_2024_partition_cluster`;

