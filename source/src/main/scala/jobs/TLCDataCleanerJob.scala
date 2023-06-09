package jobs

import consts.ArgumentsName.{INPUT_PATH, OUTPUT_PATH}
import consts.Formats
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import utils.SchemaDefinitions
import utils.WriteUtils.saveDataframe
import tranformations.DataCleanerTransf._
import tranformations.DataPreProcessingTransf._

import java.nio.file.{Files, Paths}

object TLCDataCleanerJob {
  /**
   * Cleans the input DataFrame by applying transformations to improve the quality of the data.
   *
   * @param spark The SparkSession instance.
   * @param args A map of arguments containing inputPath and outputPath.
   */
  def run(spark: SparkSession, args: Map[String, String]): Unit = {
    val inputPath: String = args.getOrElse(INPUT_PATH, "")
    val outputPath: String = args.getOrElse(OUTPUT_PATH, "")

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

    // Enhance the data with new columns
    val processedDataframe = tripDataDf
      .transform(addDurationColumn)
      .transform(addIsWeekendColumn)
      .transform(addDateColumns)

    // Clean dataframe and get a new one with better data
    val cleanDataframe = processedDataframe
      .transform(replaceNullsWithDefaults)
      .transform(filterRealisticTripsByDistance)
      .transform(filterTripsWithPassengers)
      .transform(removeTripsInDifferentDays)

    saveDataframe(cleanDataframe, outputPath)
  }
}
