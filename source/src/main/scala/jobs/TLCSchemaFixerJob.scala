package jobs

import consts.ArgumentsName
import consts.ArgumentsName.{INPUT_PATH, OUTPUT_PATH}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.input_file_name
import tranformations.ColumnMappingTransf._

import java.nio.file.{Files, Paths}
import scala.collection.parallel.CollectionConverters.ArrayIsParallelizable
import scala.collection.parallel.mutable.ParArray

object TLCSchemaFixerJob {
  /**
   * Set schema columns to match the same data type across all the Parquet files.
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

    // Get a list of Parquet files in the inputPath folder and its folders
    val parquetFiles = spark.read
      .format("parquet")
      .option("recursiveFileLookup", "true")
      .load(inputPath)
      .select(input_file_name().as("filePath"))
      .distinct()
      .collect()

    val convertedDfMap = parquetFiles.par.map { row =>
      val filePath = row.getString(0)

      val df = spark.read.parquet(filePath)

      val convertedData = df
        .transform(convertAndMapColumns)
        .transform(renameColumnsMaps)

      val fileName = filePath.split("_").last

      val modifiedFileName = fileName.replace(".parquet", "")

      modifiedFileName -> convertedData
    }

    SaveConvertedDataFrame(convertedDfMap, outputPath)
  }

  private def SaveConvertedDataFrame(convertedDfMap: ParArray[(String, Dataset[Row])], outputPath: String): Unit = {
    convertedDfMap.par.foreach { case (fileName, df) =>
      df.write
        .mode(SaveMode.Overwrite)
        .parquet(outputPath + "/" + fileName)
    }
  }
}
