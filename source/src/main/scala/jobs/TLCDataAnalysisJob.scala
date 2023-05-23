package jobs

import consts.ArgumentsName
import consts.ArgumentsName.INPUT_PATH
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Paths}

object TLCDataAnalysisJob {
  def run(spark: SparkSession, args: Map[String, String]): Unit = {
    val inputPath: String = args.getOrElse(INPUT_PATH, "")

    // Check if path exists
    if (!Files.exists(Paths.get(inputPath))) {
      print(s"Input path $inputPath does not exists.")
      sys.exit(1)
    }
  }

  // TODO: Weekend vs Weekday trips
  // TODO: Most used payment method.
  // TODO: Pick hours, Pick hours weekend vs weekday
  // TODO:
}
