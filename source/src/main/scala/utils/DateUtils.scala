package utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.unix_timestamp

object DateUtils {
  /**
   * Calculates the difference in seconds between two dates.
   *
   * @param endDate   The end date.
   * @param startDate The start date.
   * @return The difference in seconds between the end date and the start date.
   */
  def getDiffBetweenDates(endDate: Column, startDate: Column): Column = {
    unix_timestamp(endDate) - unix_timestamp(startDate)
  }
}
