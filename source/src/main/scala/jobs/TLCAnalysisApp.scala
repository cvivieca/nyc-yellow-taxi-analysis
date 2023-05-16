package jobs

import consts.{ArgumentsName, Formats, Jobs}
import org.apache.spark.sql.SparkSession
import tranformations.DataCleanerTransf
import utils._

import java.nio.file.{Files, Paths}

object TLCAnalysisApp {
  def main(args: Array[String]): Unit = {
    val argsMap: Map[String, String] = args.flatMap(StringUtils.convertArgsToMap).toMap

    val jobName: String = argsMap.getOrElse(ArgumentsName.JOB_NAME, "")

    if(jobName.isBlank || jobName.isEmpty) {
      print("Usage: Need to provide jobName parameter.")
      sys.exit(1)
    }

    val spark = SparkUtils.getSparkSession(jobName, "local[*]")

    jobName match {
      case Jobs.SCHEMA_FIXER_JOB            => TLCSchemaFixerJob.run(spark, argsMap)
      case Jobs.DATA_CLEANER_AND_PROCESSOR  => TLCDataCleanerJob.run(spark, argsMap)
      case Jobs.DATA_ANALYSIS               => TLCDataAnalysisJob.run(spark, argsMap)
      case _ =>
    }

    spark.stop()
  }
}
