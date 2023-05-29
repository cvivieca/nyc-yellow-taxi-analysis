package jobs

import consts.{Formats, PaymentTypes}
import consts.ArgumentsName.{INPUT_PATH, OUTPUT_PATH, TAXI_ZONE_PATH}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import utils.DateUtils.getMonthName
import utils.WriteUtils.saveDataframe
import utils.SchemaDefinitions

import java.nio.file.{Files, Paths}

object TLCDataAnalysisJob {
  def run(spark: SparkSession, args: Map[String, String]): Unit = {
    val inputPath: String = args.getOrElse(INPUT_PATH, "")
    val outputPath: String = args.getOrElse(OUTPUT_PATH, "")
    val taxiZonePath: String = args.getOrElse(TAXI_ZONE_PATH, "")

    // Check if path exists
    if (!Files.exists(Paths.get(taxiZonePath))) {
      print(s"Taxi Zone file $taxiZonePath does not exists.")
      sys.exit(1)
    }

    if (!Files.exists(Paths.get(inputPath))) {
      print(s"Input path $inputPath does not exists.")
      sys.exit(1)
    }

    // Get TLC Trip record schema
    val tripDataSchema = SchemaDefinitions.GetTLCCleanedDataSchema()
    val taxiZoneSchema = SchemaDefinitions.GetTaxiZoneSchema()

    // Read in TLC Trip Record Data CSV file into DataFrame
    val tripDataDf = spark.read
      .schema(tripDataSchema)
      .option("recursiveFileLookup", "true") // Reads into each year folder
      .option("dateFormat", Formats.ORIGINAL_TLC_DATE_FORMAT)
      .parquet(inputPath)

    val taxiZoneDf = spark.read
      .schema(taxiZoneSchema)
      .option("header", "true")
      .csv(taxiZonePath)

    // Get analysis results
    val averageByHourOnWeekends = tripsAverageByHour(tripDataDf, on_weekends = true)
    val averageByHourOnWeekdays = tripsAverageByHour(tripDataDf, on_weekends = false)
    val averageTripsByBoroughResult = averageTripsByBoroughAndMonth(tripDataDf, taxiZoneDf)
    val averageTripsByYearResult = tripsAverageByYear(tripDataDf)

    // Call saveDataframe for each result
    saveDataframe(averageByHourOnWeekends, outputPath, "average_by_hour_weekends")
    saveDataframe(averageByHourOnWeekdays, outputPath, "average_by_hour_weekdays")
    saveDataframe(averageTripsByBoroughResult, outputPath, "average_trips_by_borough")
    saveDataframe(averageTripsByYearResult, outputPath, "average_trips_by_year")
  }

  private def averageTripsByBoroughAndMonth(tripsDf: DataFrame, taxiZoneDf: DataFrame): DataFrame = {
    // Calculate the average trip distance by borough
    val joinedDf = joinedPickupDropOffWithTaxiZone(tripsDf, taxiZoneDf)
      .select("pickup_borough", "month", "trip_distance", "trip_duration_in_minutes")

    // Calculate average trips by hour on Weekdays
    val totalTripsByBorough = joinedDf
      .groupBy("pickup_borough", "month")
      .agg(
        count("*").as("Total Trips")
      )
      .withColumnRenamed("pickup_borough", "Pickup Borough")
      .orderBy("Pickup Borough")

    val uniqueBorough = joinedDf.select("pickup_borough").distinct().count()

    val avgTripsByBorough = totalTripsByBorough
      .withColumn("Avg. Trips", round(col("Total Trips") / uniqueBorough))

    avgTripsByBorough
  }

  private def tripsAverageByYear(tripsDf: DataFrame): DataFrame = {
    // Calculate average trips by hour on Weekdays
    val totalTripsByYear = tripsDf
      .groupBy("year")
      .agg(
        count("*").as("Total Trips")
      )
      .withColumnRenamed("year", "Year")
      .orderBy("Year")

    val uniqueYears = tripsDf.select("Year").distinct().count()

    val avgTripsByYear = totalTripsByYear
      .withColumn("Avg. Trips", round(col("Total Trips") / uniqueYears))

    avgTripsByYear
  }

  private def tripsAverageByHour(tripsDf: DataFrame, on_weekends: Boolean): DataFrame = {
    // Calculate average trips by hour on Weekdays
    val totalTripsByHour = tripsDf
      .filter(col("is_weekend") === on_weekends)
      .groupBy("hour")
      .agg(
        count("*").as("Total Trips")
      )
      .withColumnRenamed("hour", "Hour")
      .orderBy("Hour")

    val numUniqueHours = tripsDf.select("hour").distinct().count()

    val avgTripsByHour = totalTripsByHour
      .withColumn("Avg. Trips", round(col("Total Trips") / numUniqueHours))

    avgTripsByHour
  }

  private def mostUsedPaymentTypeByDayType(tripsDf: DataFrame): DataFrame = {
    // Most used payment type by weekend and weekdays
    val mappedDf = mapPaymentType(tripsDf)

    val isWeekendPayment = mappedDf
      .groupBy("is_weekend", "payment_type")
      .agg(count("*").as("payment_count"))
      .orderBy(col("is_weekend"), col("payment_count").desc)
      .groupBy("is_weekend")
      .agg(first("payment_type").as("Most Used Payment Type"))
      .withColumn("Day Type", when(col("is_weekend"), lit("Weekend")).otherwise(lit("Weekday")).cast(StringType))
      .drop("is_weekend")

    isWeekendPayment
  }

  private def mostUsedPaymentTypeByMonth(tripsDf: DataFrame): DataFrame = {
    // Most used payment type by month
    val getMonthNameByNumber: UserDefinedFunction = udf((monthNumber: Int) => getMonthName(monthNumber))

    val mostUsedPaymentByMonth = tripsDf
      .groupBy("month", "payment_type")
      .agg(count("*").as("payment_count"))
      .orderBy(col("month"), col("payment_count").desc)
      .groupBy("month")
      .agg(first("payment_type").as("Most Used Payment Type"))
      .withColumn("Month", getMonthNameByNumber(col("month")))

    mostUsedPaymentByMonth
  }

  private def joinedPickupDropOffWithTaxiZone(tripsDf: DataFrame, taxiZoneDf: DataFrame): DataFrame = {
    val pickupZoneAlias = taxiZoneDf.alias("pickupZone")
    val dropOffZoneAlias = taxiZoneDf.alias("dropoffZone")

    val joinedDf = tripsDf
      .join(pickupZoneAlias, col("pickup_location_id") === pickupZoneAlias("LocationID"), "inner")
      .withColumnRenamed("Borough", "pickup_borough")
      .join(dropOffZoneAlias, col("dropoff_location_id") === dropOffZoneAlias("LocationID"), "inner")
      .withColumnRenamed("Borough", "dropoff_borough")

    joinedDf
  }

  private def mapPaymentType(dataFrame: DataFrame): DataFrame = {
    val getPaymentTypeName: UserDefinedFunction = udf((paymentType: Int) => PaymentTypes.Types.getOrElse(paymentType, "None"))
    val updatedDf = dataFrame.withColumn("payment_type", getPaymentTypeName(col("payment_type")))

    updatedDf
  }
}
