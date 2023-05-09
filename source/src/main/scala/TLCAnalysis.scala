import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.nio.file.{Files, Paths}

object TLCAnalysis {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession object
    val spark = SparkSession.builder()
      .appName("NYC Yellow Taxi Analysis")
      .master("local[*]")
      .getOrCreate()

    // Verify there is at least one argument
    if (args.length < 1) {
      print("Usage: TLCAnalysis <tlc_trip_files_dataset>")
      sys.exit(1)
    }

    // Get the TLC files locations
    val tlcFilesPath = args(0)

    // Check if path exists
    if (!Files.exists(Paths.get(tlcFilesPath))){
      print("Path does not exists.")
      sys.exit(1)
    }

    // Get TLC Trip record schema
    val tripDataSchema = TLCDefinitions.GetSchema()

    // Read in TLC Trip Record Data CSV file into DataFrame
    val tripDataDf = spark.read
      .format("parquet")
      .option("header", "true")
      .option("recursiveFileLookup", "true") // Reads into each year folder
      .schema(tripDataSchema)
      .load(tlcFilesPath)

    // Stop SparkSession
    spark.stop()
  }
}
