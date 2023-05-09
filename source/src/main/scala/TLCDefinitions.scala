import org.apache.spark.sql.types._

object TLCDefinitions {
  def GetSchema(): StructType = {
    val tripDataSchema = StructType(Array(
      StructField("VendorID", LongType, nullable = true),
      StructField("tpep_pickup_datetime", TimestampType, nullable = true),
      StructField("tpep_dropoff_datetime", TimestampType, nullable = true),
      StructField("passenger_count", LongType, nullable = true),
      StructField("trip_distance", DoubleType, nullable = true),
      StructField("RatecodeID", LongType, nullable = true),
      StructField("store_and_fwd_flag", StringType, nullable = true),
      StructField("PULocationID", LongType, nullable = true),
      StructField("DOLocationID", LongType, nullable = true),
      StructField("payment_type", LongType, nullable = true),
      StructField("fare_amount", DoubleType, nullable = true),
      StructField("extra", DoubleType, nullable = true),
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
}
