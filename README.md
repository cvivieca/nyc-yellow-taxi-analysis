


# NYC Yellow Taxi Analysis

This project analyzes NYC yellow taxi trip data using Apache Spark and Scala.

## Getting the Data

To obtain the data, please follow the instructions in the repository: [TLC Trip Record Data Downloader](https://github.com/cvivieca/tlc_trip_record_data_downloader).

Alternatively, you can access the NYC Taxi & Limousine Commission (TLC) trip record data directly from the [official website](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

## Tools Used

- Apache Spark
- Scala


## General Description
The dataset covers the period from 2017 to 2022 and contains:

### Dataset Details:
- Number of rows: 396,179,656
- Number of columns: 19
- Size of data: 5.79 GB.

## Resolving Schema Mismatch Issue Prior to Processing
After downloading the data, I discovered that the Parquet metadata differs among the Parquet files. For instance, some files have the `passenger_count` or `congestion_surcharge` stored as a `DOUBLE` type, while others use `INT64`.

To address this inconsistency, a job has been implemented to reconcile the schemas across the Parquet files. You can find the implementation of this job under the directory `source/src/main/scala/jobs`.

In summary, the job reads each Parquet file and applies a mapping rule to match the schema based on predefined rules. Additionally, it performs column renaming as part of the schema reconciliation process.

#### Mapping rules
``` scala
val columnConvertMapping = Map(  
	"passenger_count" -> ByteType,  
	"payment_type" -> ByteType,  
	"congestion_surcharge" -> DoubleType,  
	"airport_fee" -> DoubleType,  
	"RatecodeID" -> ByteType,  
	"VendorID" -> ByteType,  
	"PULocationID" -> ShortType,  
	"DOLocationID" -> ShortType
)

val columnRenameMapping = Map(  
	"RatecodeID" -> "rate_code_id",  
	"VendorID" -> "vendor_id",  
	"PULocationID" -> "pickup_location_id",  
	"DOLocationID" -> "dropoff_location_id",  
	"extra" -> "extra_charges"  
)

// more code ...

val convertedDfMap = parquetFiles.par.map { row =>  
	val filePath = row.getString(0)  
	  
	val df = spark.read.parquet(filePath) 
	 
	val convertedData = df  
		.transform(convertAndMapColumns)  
		.transform(renameColumnsMaps)  
	  
	val fileName = filePath.split("_").last  
	  
	val modifiedFileName = fileName.replace(".parquet", "")  

	modifiedFileName -> convertedData  
}

// yes, more code ...
```

Run this spark job before against your downloaded dataframe. If just one Parquet file was downloaded this step is not needed because one parquet file have a schema defined, this will fix schema mismatch between multiptle files. 

```shell
spark-submit --class com.jobs.TLCAnalysisApp --master local[*] TLCAnalysisApp.jar SchemaFixerJob input_data_path output_date_path
```

> Note: Using `option("mergeSchema", "true")` will not resolve the issue as the types vary between INT64 and DOUBLE, which would lead to a conversion error.


## Reading the data

```scala
// Read in TLC Trip Record Data CSV file into DataFrame
val tripDataDf = spark.read
  .format("parquet")
  .option("recursiveFileLookup", "true") // Reads into each year folder
  .schema(tripDataSchema)
  .load(tlcFilesPath)
```

### Schema
The schema is defined manually in `TLCDefinitions.scala`
```scala
val tripDataSchema = StructType(Array(  
	StructField("vendor_id", ByteType, nullable = true),
	StructField("tpep_pickup_datetime", TimestampType, nullable = true),  
	StructField("tpep_dropoff_datetime", TimestampType, nullable = true),  
	StructField("passenger_count", ByteType, nullable = true),  
	StructField("trip_distance", DoubleType, nullable = true),  
	StructField("rate_code_id", ByteType, nullable = true),  
	StructField("store_and_fwd_flag", StringType, nullable = true),  
	StructField("pickup_location_id", ShortType, nullable = true),
	StructField("dropoff_location_id", ShortType, nullable = true),
	StructField("payment_type", ByteType, nullable = true),  
	StructField("fare_amount", DoubleType, nullable = true),  
	StructField("extra_charges", DoubleType, nullable = true),  
	StructField("mta_tax", DoubleType, nullable = true),  
	StructField("tip_amount", DoubleType, nullable = true),  
	StructField("tolls_amount", DoubleType, nullable = true),  
	StructField("improvement_surcharge", DoubleType, nullable = true),  
	StructField("total_amount", DoubleType, nullable = true),  
	StructField("congestion_surcharge", DoubleType, nullable = true),  
	StructField("airport_fee", DoubleType, nullable = true)  
))
```

#### Fields Description
| Field Name            | Description                                                                                                                               |
|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| vendor_id              | A code indicating the TPEP provider that provided the record. 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc.                       |
| tpep_pickup_datetime  | The date and time when the meter was engaged.                                                                                              |
| tpep_dropoff_datetime | The date and time when the meter was disengaged.                                                                                            |
| passenger_count       | The number of passengers in the vehicle. This is a driver-entered value.                                                                    |
| trip_distance         | The elapsed trip distance in miles reported by the taximeter.                                                                              |
| pickup_location_id          | TLC Taxi Zone in which the taximeter was engaged.                                                                                           |
| dropoff_location_id          | TLC Taxi Zone in which the taximeter was disengaged.                                                                                        |
| rate_code_id            | The final rate code in effect at the end of the trip. 1= Standard rate, 2=JFK, 3=Newark, 4=Nassau or Westchester, 5=Negotiated fare, 6=Group ride. |
| store_and_fwd_flag    | This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka “store and forward.” Y= store and forward trip, N= not a store and forward trip. |
| payment_type          | A numeric code signifying how the passenger paid for the trip. 1= Credit card, 2= Cash, 3= No charge, 4= Dispute, 5= Unknown, 6= Voided trip. |
| fare_amount           | The time-and-distance fare calculated by the meter.                                                                                        |
| extra_charges                 | Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges.                      |
| mta_tax               | $0.50 MTA tax that is automatically triggered based on the metered rate in use.                                                            |
| improvement_surcharge | $0.30 improvement surcharge assessed trips at the flag drop. The improvement surcharge began being levied in 2015.                         |
| tip_amount            | his field is automatically populated for credit card tips. Cash tips are not included.                                        |
| tolls_amount          | Total amount of all tolls paid in trip.                                                                                                    |
| total_amount          | The total amount charged to passengers. Does not include cash tips.                                                                        |
| congestion_surcharge  | Total amount collected in trip for NYS congestion surcharge.                                                                               |
| airport_fee           | $1.25 for pick up only at LaGuardia and John F. Kennedy Airports.                                                                           |

### Field description reference
NYC Taxi & Limousine Commission. (n.d.). Data Dictionary - Trip Records - Yellow Taxi.
Retrieved May 9, 2023, from https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

# Data cleansing and outliners detection 

## 1. Remove inconsistent trips
Upon reviewing the DataFrame, several rows appear to have irregular values. 

#### 1.1 Out of range trips
The columns `pickup_location_id` and `dropoff_location_id` represent the boroughs where the passengers were picked up (PU) and dropped off (DO). In the table below, the first row displays a `trip_distance` of `389678.46` miles, but the pickup and drop-off locations are the same or near one to the other. 

It is highly unlikely to have such trips within the same city, as they would require several days to complete.
```
+------------------+-------------------+--------------------+---------------------+---------------+-------------+------------+
|pickup_location_id|dropoff_location_id|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|total_amount|
+------------------+-------------------+--------------------+---------------------+---------------+-------------+------------+
|               238|                140| 2022-10-28 01:19:00|  2022-10-28 01:32:00|           null|    389678.46|       18.83|
|               249|                137| 2022-05-15 14:45:00|  2022-05-15 14:57:00|           null|    357192.65|       20.13|
|               226|                260| 2021-11-16 08:55:00|  2021-11-16 09:00:00|           null|    351613.36|       19.88|
|               219|                170| 2020-12-10 04:39:00|  2020-12-10 05:14:00|           null|    350914.89|       57.35|
|               175|                232| 2020-12-09 04:57:00|  2020-12-09 05:30:00|           null|    350814.14|       63.42|
|                38|                 16| 2020-12-09 04:22:00|  2020-12-09 04:37:00|           null|     350793.6|       34.09|
|               201|                 61| 2020-12-08 06:59:00|  2020-12-08 07:35:00|           null|    350722.34|        60.0|
|                22|                150| 2020-12-08 05:16:00|  2020-12-08 05:27:00|           null|    350696.98|        21.2|
|               203|                135| 2020-12-03 04:15:00|  2020-12-03 04:37:00|           null|    350104.58|       34.09|
|               222|                 29| 2020-12-02 04:52:00|  2020-12-02 05:03:00|           null|    349987.05|       31.27|
|                19|                 78| 2020-11-29 10:54:00|  2020-11-29 11:17:00|           null|     349692.3|       52.11|
|                13|                107| 2022-02-15 14:24:00|  2022-02-15 14:37:00|           null|    348798.53|        27.9|
|               144|                239| 2021-10-27 12:46:00|  2021-10-27 13:24:00|           null|    345124.27|       34.67|
|               170|                132| 2022-05-19 13:00:00|  2022-05-19 14:20:00|           null|    344408.48|       73.97|
|               219|                 56| 2021-12-11 06:48:00|  2021-12-11 07:11:00|           null|     335093.7|       39.67|
|               140|                264| 2021-11-16 05:02:00|  2021-11-16 05:22:00|           null|    334779.46|        20.0|
|               218|                228| 2021-12-07 03:05:00|  2021-12-07 04:15:00|           null|     334236.2|       66.94|
|                24|                 43| 2022-05-03 05:33:00|  2022-05-03 05:55:00|           null|    333632.96|        23.1|
|               212|                 62| 2021-11-29 00:01:00|  2021-11-29 00:33:00|           null|    333152.56|       53.51|
|               193|                140| 2021-05-11 03:36:00|  2021-05-11 03:57:00|           null|    332541.19|       25.63|
+------------------+-------------------+--------------------+---------------------+---------------+-------------+------------+
```

In Jupyter Notebook, I filtered out trip distances based a on range of 0.0001 miles as the minimum trip distance and 100 miles as the maximum. 

```text
Total rows out of range: 3,520,250 (0.89% of total)
Total rows in range 392,659,409
Total count: 396,179,656
```

```scala
/**  
* Removes unrealistic trips from the given DataFrame.  
*  
* @param dataframe The DataFrame containing trip data.  
* @return A new DataFrame with consistent trips that meet the specified criteria.  
*/  
def filterRealisticTripsByDistance(dataframe: DataFrame): DataFrame = {  
	// Get trips that are between the ranges expression  
	val tripDistanceColumn = col("trip_distance")  
	val minAndMaxDistanceExp = tripDistanceColumn >= AppConstants.MIN_TRIP_DISTANCE and tripDistanceColumn <= AppConstants.MAX_TRIP_DISTANCE  
	  
	// Get trips filter by the expressions  
	val realisticTripsDf = dataframe.filter(minAndMaxDistanceExp)  
	  
	realisticTripsDf  
}
```

#### 1.2 Out of range passenger count

In compliance with regulations, taxi cabs are restricted to carrying a maximum of 5 passengers. To adhere to this rule, I filtered out trips with more than 5 passengers or less than 1 passenger.

The resulting dataset provides the following breakdown:

```text
Total rows out of range: 3,520,250 (3.57% of total)
Total rows in range: 392,659,406
```

For more information on passenger regulations, please refer to the [NYC Taxi and Limousine Commission](https://www.nyc.gov/site/tlc/passengers/passenger-frequently-asked-questions.page) website.

```scala
/**  
* Filters out trips from the given DataFrame that do not have at least one passenger.  
*  
* @param dataframe The DataFrame containing trip data.  
* @return A new DataFrame with trips that have at least one passenger.  
*/  
def filterTripsWithPassengers(dataframe: DataFrame): DataFrame = {  
	// Get trips where passenger count is lower than the maximum allowed and at least one passenger  
	val passengerCountColumn = col("passenger_count")  
	val maxPassengerCountAllowedExp = passengerCountColumn < AppConstants.MAX_PASSENGER_COUNT_ALLOWED  
	val atLeastOnePassengerExp = passengerCountColumn > 0  
	  
	val passengerCountFilterExp = maxPassengerCountAllowedExp and atLeastOnePassengerExp  
	  
	// Get trips filter by the expressions  
	val filterTripsWithPassengersDf = dataframe.filter(passengerCountFilterExp)  
	  
	filterTripsWithPassengersDf  
}
```

## 2. Set default values
In certain columns, null values exist for numerical data. For instance, to address null values in the `payment_type` column, I will assign the default value "unknown" instead.

To handle this, the following function is implemented to replace null values with default values:

```scala
/**  
* Cleans the given DataFrame by setting null values to its default value  
*  
* @param dataframe The DataFrame containing trip data.  
* @return A new DataFrame with null values replaced by its default value  
*/  
def replaceNullsWithDefaults(dataframe: DataFrame): DataFrame = {  
	val replacedValuesDf = dataframe.na.fill(Map(  
	"congestion_surcharge" -> 0.0,  
	"airport_fee" -> 0.0,  
	"payment_type" -> PaymentTypes.UNKNOWN  
	))  
	  
	replacedValuesDf  
}
```