package tranformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ByteType, DoubleType, ShortType}

object ColumnMappingTransf {
  /**
   * Convert columns in the given DataFrame to match the specified column types based on a predefined mapping.
   *
   * @param dataFrame The input DataFrame to convert columns for.
   * @return A new DataFrame with columns converted to the specified types.
   */
  def convertAndMapColumns(dataFrame: DataFrame): DataFrame = {
    val columnConvertMapping = Map(
      "passenger_count" -> ByteType,
      "payment_type" -> ByteType,
      "congestion_surcharge" -> DoubleType,
      "airport_fee" -> DoubleType,
      "RatecodeID" -> ByteType,
      "VendorID" -> ByteType,
      "PULocationID" -> ShortType,
      "DOLocationID" -> ShortType
    )

    val convertedDf = dataFrame.schema.fields.foldLeft(dataFrame) { case (accDF, field) =>
      columnConvertMapping.get(field.name) match {
        case Some(newType) => accDF.withColumn(field.name, col(field.name).cast(newType))
        case None => accDF
      }
    }

    convertedDf
  }

  /**
   * Renames columns in the given DataFrame based on a predefined mapping.
   *
   * @param dataFrame The input DataFrame to rename columns for.
   * @return The DataFrame with renamed columns.
   */
  def renameColumnsMaps(dataFrame: DataFrame): DataFrame = {
    val columnRenameMapping = Map(
      "RatecodeID" -> "rate_code_id",
      "VendorID" -> "vendor_id",
      "PULocationID" -> "pickup_location_id",
      "DOLocationID" -> "dropoff_location_id",
      "extra" -> "extra_charges"
    )

    dataFrame.withColumnsRenamed(columnRenameMapping)
  }
}