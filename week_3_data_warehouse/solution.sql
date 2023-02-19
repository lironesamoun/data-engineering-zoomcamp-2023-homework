  -- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE
  `fhvtrip.external_fhv_tripdata_2020` OPTIONS ( format = 'PARQUET',
    uris = ['gs://dtc-trip-data/2020/fhv_tripdata_2020-*.parquet'] );
  -- Check fhv trip data
SELECT
  *
FROM
  fhvtrip.external_fhv_tripdata_2020
LIMIT
  10;
  -- Count total data
SELECT
  COUNT(*)
FROM
  fhvtrip.external_fhv_tripdata_2019;

  -- Create a non partitioned table from external table
CREATE OR REPLACE TABLE
  fhvtrip.fhv_tripdata_2019_non_partitoned AS
SELECT
  *
FROM
  fhvtrip.external_fhv_tripdata_2019;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE
  fhvtrip.fhv_tripdata_2019_non_partitoned
PARTITION BY
  DATE(pickup_datetime) AS
SELECT
  *
FROM
  fhvtrip.external_yfhv_tripdata_2019;
  -- Scanning 322.67MB of data
SELECT
  DISTINCT(affiliated_base_number)
FROM
  fhvtrip.fhv_tripdata_2019_non_partitoned;


SELECT
  DISTINCT(affiliated_base_number)
FROM
  `fhvtrip.external_yellow_tripdata_parquet`;

SELECT count(*) FROM fhvtrip.fhv_tripdata_2019_non_partitoned
WHERE PUlocationID is null and DOlocationID is null;

-- Partition by pickup_datetime Partition by affiliated_base_number
CREATE OR REPLACE TABLE fhvtrip.fhv_tripdata_2019_partitoned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM fhvtrip.external_fhv_tripdata_2019;

  -- Scanning 652.6MB of data for non partitioned
SELECT
  DISTINCT(affiliated_base_number)
FROM
  fhvtrip.fhv_tripdata_2019_non_partitoned
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

  -- Scanning 23.07MB of data for partitioned and clustered table
SELECT
  DISTINCT(affiliated_base_number)
FROM
  fhvtrip.fhv_tripdata_2019_partitoned_clustered
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

