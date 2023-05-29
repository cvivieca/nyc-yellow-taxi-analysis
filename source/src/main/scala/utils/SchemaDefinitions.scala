package utils

import org.apache.spark.sql.types._

object SchemaDefinitions {
  def GetTLCSchema(): StructType = {
    val tripDataSchema = StructType(Array(
      StructField("vendor_id", ByteType, nullable = true),
      StructField("tpep_pickup_datetime", TimestampType, nullable = true),
      StructField("tpep_dropoff_datetime", TimestampType, nullable = true),
      StructField("passenger_count", ByteType, nullable = true),
      StructField("trip_distance", DoubleType, nullable = true),
      StructField("rate_code_id", ByteType, nullable = true),
      StructField("store_and_fwd_flag", StringType, nullable = true),
      StructField("pickup_location_id", ShortType, nullable = true),   // PULocationID
      StructField("dropoff_location_id", ShortType, nullable = true),  // DOULocationID
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

    tripDataSchema
  }

  def GetTLCCleanedDataSchema(): StructType = {
    val originalSchema = GetTLCSchema()

    originalSchema
      .add(StructField("trip_duration_in_minutes", DoubleType, nullable = true))
      .add(StructField("month", ByteType, nullable = true))
      .add(StructField("day", ShortType, nullable = true))
      .add(StructField("year", IntegerType, nullable = true))
      .add(StructField("hour", ByteType, nullable = true))
      .add(StructField("is_weekend", BooleanType, nullable = true))
  }

  def GetTaxiZoneSchema(): StructType = {
    val taxiZoneSchema = StructType(Array(
      StructField("LocationID", ShortType, nullable = false),
      StructField("Borough", StringType, nullable = false),
      StructField("Zone", StringType, nullable = false),
      StructField("service_zone", StringType, nullable = false)
    ))

    taxiZoneSchema
  }
}