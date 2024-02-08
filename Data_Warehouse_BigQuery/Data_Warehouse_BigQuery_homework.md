# Data Warehouse BigQuery - Week 3 Homework
#### Maria Fisher 

## Instructions 
<b><u>Important Note:</b></u> <p> For this homework we will be using the 2022 Green Taxi Trip Record Parquet Files from the New York
City Taxi Data found here: </br> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page </br>
If you are using orchestration such as Mage, Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You will need to use the PARQUET option files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the Green Taxi Trip Records Data for 2022. </br>
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table). </br>
</p>
---------------------------
CREATE OR REPLACE TABLE de-zoomcamp-412914.green_taxi_2022.gt_non_partitoned AS
SELECT * FROM `de-zoomcamp-412914.green_taxi_2022.g2_taxi_2022`;
-----------------------------------

## Question 1:
Question 1: What is count of records for the 2022 Green Taxi Data??
- 65,623,481
- 840,402 (x)
- 1,936,423
- 253,647

## Question 2:

---
Query:
SELECT COUNT(DISTINCT PULocationID) AS TotalDistinctPULocationIDs
FROM `de-zoomcamp-412914.green_taxi_2022.g2_taxi_22`;

SELECT COUNT(DISTINCT PULocationID) AS TotalDistinctPULocationIDs
FROM `de-zoomcamp-412914.green_taxi_2022.gt_non_partitoned`;
---
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table (x)
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table 


## Question 3:
---
Query:
SELECT COUNT(*) AS ZeroFareCount
FROM `de-zoomcamp-412914.green_taxi_2022.g2_taxi_22`
WHERE fare_amount = 0;
---
How many records have a fare_amount of 0?
- 12,488
- 128,219
- 112
- 1,622 (x)

## Question 4:

-----
Query:
CREATE  OR REPLACE TABLE de-zoomcamp-412914.green_taxi_2022.taxi_partition
PARTITION BY (lpep_pickup_date)
CLUSTER BY PULocationID AS
SELECT *
FROM `de-zoomcamp-412914.green_taxi_2022.g2_taxi_22`;
---
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
- Cluster on lpep_pickup_datetime Partition by PUlocationID
- Partition by lpep_pickup_datetime  Cluster on PUlocationID (x)
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

## Question 5:
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table (x)
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table


## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket  x
- Big Table
- Container Registry


## Question 7:
It is best practice in Big Query to always cluster your data:
- True x
- False


## (Bonus: Not worth points) Question 8:
No Points: Write a SELECT count(*) query FROM the materialized table you created.
How many bytes does it estimate will be read? 
0 bytes

The query results indicate that no bytes were consumed for reading. 
However, it's important to note that a materialized table in BigQuery still occupies storage space in Google Cloud Platform's infrastructure. The misconception may arise from the fact that querying a materialized table doesn't result in additional charges for bytes read if the query's results are already stored in the table. However, it does consume resources and contribute to billing based on the amount of data read.

 
## Submitting the solutions

* Form for submitting: TBD
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: TBD


