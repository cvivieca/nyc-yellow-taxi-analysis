package jobs

import consts.{ArgumentsName, Formats}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import utils.SchemaDefinitions
import tranformations.DataCleanerTransf._

import java.nio.file.{Files, Paths}

object TLCDataCleanerJob {
  /**
   * Cleans the input DataFrame by applying transformations to improve the quality of the data.
   *
   * @param spark The SparkSession instance.
   * @param args A map of arguments containing inputPath and outputPath.
   */
  def run(spark: SparkSession, args: Map[String, String]): Unit = {
    val inputPath: String = args.getOrElse(ArgumentsName.INPUT_PATH, "")
    val outputPath: String = args.getOrElse(ArgumentsName.OUTPUT_PATH, "")

    // Check if path exists
    if (!Files.exists(Paths.get(inputPath))) {
      print(s"Input path $inputPath does not exists.")
      sys.exit(1)
    }

    // Get TLC Trip record schema
    val tripDataSchema = SchemaDefinitions.GetTLCSchema()

    // Read in TLC Trip Record Data CSV file into DataFrame
    val tripDataDf = spark.read
      .schema(tripDataSchema)
      .option("recursiveFileLookup", "true") // Reads into each year folder
      .option("dateFormat", Formats.ORIGINAL_TLC_DATE_FORMAT)
      .parquet(inputPath)

    // Clean dataframe and get a new one with better data
    val cleanDataframe = tripDataDf
      .transform(replaceNullsWithDefaults)
      .transform(filterRealisticTripsByDistance)
      .transform(filterTripsWithPassengers)

    saveCleanDataframe(cleanDataframe, outputPath)
  }

  private def saveCleanDataframe(dataFrame: DataFrame, outputPath: String): Unit = {
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath + "/" + "")
  }
}
