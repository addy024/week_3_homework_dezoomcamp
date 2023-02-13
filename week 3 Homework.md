# DE-ZOOMCAMP 2023 - Week 3

## Homework - Data Warehouse

### Question 1. What is the count for fhv vehicle records for year 2019?

```SQL
SELECT count(*) from `data-engineering-375013.dezoomcamp.external_fhv_tripdata`;
```
Answer:
```
43,244,696
```

### Question 2. What is the estimated amount of data that will be read when you execute your query on the External Table and the Materialized Table?
```SQL
SELECT count(DISTINCT(affiliated_base_number)) FROM `data-engineering-375013.dezoomcamp.external_fhv_tripdata`;

SELECT count(DISTINCT(affiliated_base_number)) FROM `data-engineering-375013.dezoomcamp.fhv_tripdata_non_partitioned`;
```
Answer:
```
0 MB for the External Table and 317.94MB for the Materialized Table
```

### Question 3. How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
```SQL
SELECT count(*) FROM `data-engineering-375013.dezoomcamp.fhv_tripdata_non_partitioned`
WHERE PUlocationID is null and DOlocationID is null;
```
Answer:
```
717,748
```

### Question 4. What is the best strategy to make an optimized table in Big Query if your query will always filter by pickup_datetime and order by affiliated_base_number?

Answer:
```
Partition by pickup_datetime Cluster on affiliated_base_number
```

### Question 5. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 
03/01/2019 and 03/31/2019 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? 
```SQL
SELECT DISTINCT(affiliated_base_number) FROM `data-engineering-375013.dezoomcamp.fhv_tripdata_non_partitioned` 
WHERE pickup_datetime BETWEEN TIMESTAMP("2019-03-01") AND TIMESTAMP("2019-03-31");

CREATE OR REPLACE TABLE `data-engineering-375013.dezoomcamp.fhv_tripdata_partitioned` 
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS (
  SELECT * FROM `data-engineering-375013.dezoomcamp.fhv_tripdata_non_partitioned`
)

SELECT DISTINCT(affiliated_base_number) FROM `data-engineering-375013.dezoomcamp.fhv_tripdata_partitioned` 
WHERE pickup_datetime BETWEEN TIMESTAMP("2019-03-01") AND TIMESTAMP("2019-03-31");

```

Answer: 
```
647.87 MB for non-partitioned table and 23.06 MB for the partitioned table
```

### Question 6. Where is the data stored in the External Table you created?

Answer:

```
GCP Bucket
```

### Question 7: It is best practice in Big Query to always cluster your data.

Answer:
```
False
```
