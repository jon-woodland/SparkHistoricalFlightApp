package org.final_project.final_project.flightUtils

import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.{LazyLogging}
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import pureconfig.generic.auto._
import org.apache.spark.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

object FlightUtils extends LazyLogging {

  /** loadData method. This method loads data into a DataFrame with the schema
    * defined in the FlightData.scala object.
    * @param spark
    *   The Spark session
    * @param inputPath
    *   The input S3 path
    * @return
    *   DataFrame loaded with data
    */
  def loadData(
      spark: SparkSession,
      inputPath: String,
      inputSchema: StructType
  ): DataFrame = {
    import spark.implicits._
    logger.info("Loading data")

    // Load data into DataFrame using spark.read
    val df =
      spark.read
        .schema(inputSchema)
        .format("CSV")
        .option("header", "true")
        .load(inputPath)
        .withColumn(
          "LoadLocation",
          input_file_name()
        ) // Append column to DF for the load location.

    // Log that data was loaded into the DataFrame
    df.inputFiles.foreach { file =>
      logger.info(s"Loaded data from file: $file")
    }
    logger.info("Done loading data")

    df
  }

  /** loadMultipleData method. This method loads multiple csvs of data into a
    * DataFrame with the schema defined in the FlightData.scala object.
    * @param spark
    *   The Spark session
    * @param inputPath
    *   The input S3 path
    * @return
    *   DataFrame loaded with data from multiple CSV files
    */
  def loadMultipleData(
      spark: SparkSession,
      inputPath: String,
      inputSchema: StructType
  ): DataFrame = {
    import spark.implicits._
    logger.info("Loading data from multiple CSV files")

    // Load data into DataFrame using spark.read with a wildcard
    val df =
      spark.read
        .schema(inputSchema)
        .format("CSV")
        .option("header", "true")
        .load(s"$inputPath/*.csv") // To iterate and grab all CSVs in the path
        .withColumn(
          "LoadLocation",
          input_file_name()
        )

    // Log that data was loaded into the DataFrame
    df.inputFiles.foreach { file =>
      logger.info(s"Loaded data from file: $file")
    }
    logger.info("Done loading data from multiple CSV files")

    df
  }

  /** This method saves an input that has been transformed and has a designated
    * output schema to a DataFrame as a Parquet file in S3.
    * @param df
    *   The DataFrame to be saved
    * @param outputPath
    *   The output S3 path
    * @param outputType
    *   The type of the output
    * @param outputSchema
    *   The schema of the output
    * @param outputFileName
    *   The name of the output file
    */
  def saveTransformedOutput(
      df: DataFrame,
      outputPath: String,
      outputType: String,
      outputSchema: StructType,
      outputFileName: String
  ): Unit = {
    // outputPath is provided in the args and output file name is passed in so that we can seperate the outputfiles into
    // their on paths
    val fullOutputPath = s"$outputPath$outputType/$outputFileName"
    logger.info(s"Saving output to file: $fullOutputPath")

    // Explicitly definine the output schema. Parquet is usually really good at inferring the schema but I wanted to explicitly
    val dfWithSchema = df
      .selectExpr(
        outputSchema.fieldNames: _*
      )

    // Write the dataframe with the schema attached as a parquet file to the s3 output location passed in
    dfWithSchema
      .coalesce(1)
      .write
      .mode(sql.SaveMode.Overwrite)
      .parquet(fullOutputPath) // Save as Parquet
    logger.info(s"Output saved to: $fullOutputPath")
  }

  /** This method saves an input DataFrame as a Parquet file in S3 in it's raw
    * form.
    * @param df
    *   The DataFrame to be saved
    * @param outputPath
    *   The output S3 path
    * @param outputType
    *   The type of the output
    * @param outputFileName
    *   The name of the output file
    */
  def saveRawOutput(
      df: DataFrame,
      outputPath: String,
      outputType: String,
      outputFileName: String
  ): Unit = {
    // outputPath is provided in the args and output file name is passed in so that we can seperate the outputfiles into
    // their on paths
    val fullOutputPath = s"$outputPath$outputType/$outputFileName"
    logger.info(s"Saving output to file: $fullOutputPath")

    // Write the dataframe, infering the schema, as a parquet file to the s3 output location passed in
    df
      .coalesce(1)
      .write
      .mode(sql.SaveMode.Overwrite)
      .parquet(fullOutputPath) // Save as Parquet
    logger.info(s"Output saved to: $fullOutputPath")
  }
}
