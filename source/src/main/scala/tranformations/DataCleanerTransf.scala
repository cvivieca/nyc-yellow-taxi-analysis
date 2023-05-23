package tranformations

import consts.{AppConstants, PaymentTypes}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, datediff, round}
import utils.DateUtils._

object DataCleanerTransf {
  /**
   * Cleans the given DataFrame by setting null values to its default value
   *
   * @param dataframe The DataFrame containing trip data.
   * @return A new DataFrame with null values replaced by its default value
   */
  def replaceNullsWithDefaults(dataframe: DataFrame): DataFrame = {
    val replacedValuesDf = dataframe.na.fill(Map(
      "congestion_surcharge"  -> 0.0,
      "airport_fee"           -> 0.0,
      "payment_type"          -> PaymentTypes.UNKNOWN
    ))

    replacedValuesDf
  }

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

  /**
   * Removes trips from the DataFrame that are in multiple days based on the pickup and dropoff timestamps.
   *
   * @param dataFrame The input DataFrame containing trip data.
   * @return A new DataFrame with trips that are within the same day.
   */
  def removeTripsInDifferentDays(dataFrame: DataFrame): DataFrame = {
    val pickupDateColumn = col("tpep_pickup_datetime")
    val dropOffDateColumn = col("tpep_dropoff_datetime")

    val daysDiff = getDiffBetweenDates(dropOffDateColumn, pickupDateColumn) / (24 * 60 * 60)

    val tripsWithSameDayDf = dataFrame.filter(round(daysDiff) <= AppConstants.TRIPS_MAX_DAYS_DIFF)

    tripsWithSameDayDf
  }
}
