package utils

import org.apache.spark.sql.SparkSession

object SparkUtils {
  /**
   * Creates and returns a SparkSession with the provided jobName and master configuration.
   *
   * @param jobName The name of the Spark job.
   * @param master The Spark master URL.
   * @return The created SparkSession instance.
   */
  def getSparkSession(jobName: String, master: String): SparkSession = {
    val spark = SparkSession.builder()
      .appName(jobName)
      .master(master)
      .getOrCreate()

    spark
  }
}
