package tranformations

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import utils.DateUtils
import utils.DateUtils.getDiffBetweenDates

object DataProcessorTransf {
  /**
   * Adds a column to the DataFrame representing the duration of each trip.
   *
   * @param dataFrame The input DataFrame.
   * @return The DataFrame with an additional "trip_duration" column.
   */
  def addDurationColumn(dataFrame: DataFrame): DataFrame = {
    val pickupDateTimeColumn = col("tpep_pickup_datetime")
    val dropOffDateTimeColumn = col("tpep_dropoff_datetime")

    val tripDurationInMinutes = getDiffBetweenDates(dropOffDateTimeColumn, pickupDateTimeColumn) / 60

    dataFrame.withColumn("trip_duration_in_minutes", tripDurationInMinutes)
  }

  /**
   * Adds an "is_weekend" column to the DataFrame based on the "tpep_pickup_datetime" column.
   *
   * @param dataFrame The input DataFrame.
   * @return A new DataFrame with the "is_weekend" column added.
   */
  def addIsWeekendColumn(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn(
      "is_weekend",
      dayofweek(col("tpep_pickup_datetime")).isin(1, 7)
    )
  }

  /**
   * Adds columns for month, day, and year based on the "tpep_pickup_datetime" column.
   *
   * @param dataFrame The input DataFrame.
   * @return A new DataFrame with columns for month, day, and year added.
   */
  def addDateColumns(dataFrame: DataFrame): DataFrame = {
    val pickupDateColumn = col("tpep_pickup_datetime")

    val dfWithDatesColumns = dataFrame.withColumns(Map(
      "month" -> month(pickupDateColumn),
      "day"   -> dayofmonth(pickupDateColumn),
      "year"  -> year(pickupDateColumn)
    ))

    dfWithDatesColumns
  }
}
