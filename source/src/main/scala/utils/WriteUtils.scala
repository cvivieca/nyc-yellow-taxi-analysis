package utils

import org.apache.spark.sql.{DataFrame, SaveMode}

object WriteUtils {
   def saveDataframe(dataFrame: DataFrame, outputPath: String, fileName: String = ""): Unit = {
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath + "/" + fileName)
  }
}
