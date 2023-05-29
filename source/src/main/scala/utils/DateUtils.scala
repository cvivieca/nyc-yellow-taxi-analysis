package utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.unix_timestamp

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

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

  /**
   * Retrieves the name of the month for the given month number.
   *
   * @param monthNumber The month number (1 to 12).
   * @return The name of the month in full.
   */
  def getMonthName(monthNumber: Int): String = {
    val calendar = Calendar.getInstance()
    calendar.set(Calendar.MONTH, monthNumber - 1)

    val dateFormat = new SimpleDateFormat("MMMM", Locale.getDefault())

    dateFormat.format(calendar.getTime())
  }
}
