package org.final_project.final_project

import org.final_project.final_project.flightUtils.FlightUtils
import org.final_project.final_project.flightData.FlightData
import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.{LazyLogging}
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import pureconfig.generic.auto._
import org.apache.spark.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

/** The main object for the Spark application for historical flight data
  * analysis.
  */
object SparkHistoricalFlightApp extends LazyLogging {

  /** The main entry point for the Spark application.
    * @param args
    *   Example of command-line arguments (inputPath, outputPath, [optional]
    *   "full", referenceTablePath, carrierSummaryPath)
    */
  def main(args: Array[String]): Unit = {

    // Step 1: Establish Spark connection
    val spark = SparkSession
      .builder()
      .appName("final-project-spark")
      .config("spark.executor.memory", "4g") // Set the executor memory
      .config("spark.dynamicAllocation.enabled", "true")
      .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
      )
      .getOrCreate()

    import spark.implicits._

    logger.info("Parsing args")
    // Step 2: Parse command-line arguments
    val inputPath = args(0)
    val outputPath = args(1)
    val referenceTablePath = args(3)
    val carrierSummaryPath = args(4)

    // Step 3: List CSV files in the reference table folder
    logger.info("Collecting reference tables")
    val csvFilesDF = spark.read
      .option("header", "true")
      .csv(referenceTablePath + "/*.csv")

    // Extract file names using input_file_name()
    val filePaths = csvFilesDF
      .withColumn("FileName", input_file_name())
      .select("FileName")
      .distinct()
      .as[String]
      .collect()
    logger.info(s"Found reference tables ${filePaths}")

    // Step 4: Loop through each reference path and load and save as parquet
    logger.info("Processing reference tables")
    filePaths.foreach { path =>
      // Extract the table name from the path
      val tableName = path.split("/").last.stripSuffix(".csv")
      logger.info(s"Table name: ${tableName}")

      // Use a match expression to determine the appropriate schema
      val inputSchema: StructType = tableName match {
        case "CarrierMapping" => CarrierMapping.schema
        case "OriginMapping"  => OriginMapping.schema
        // Add more cases for additional reference tables
        case _ =>
          throw new IllegalArgumentException(
            s"No schema defined for table: $tableName"
          )
      }

      // Load reference data
      val refLoadedData = FlightUtils.loadData(spark, path, inputSchema)

      // Save the reference data
      FlightUtils.saveRawOutput(
        refLoadedData,
        outputPath,
        "reference",
        tableName
      )
    }
    logger.info("Done saving reference tables")

    // Step 5: Load monthly flight data into a DataFrame
    // If arg(2) is passed in as full then load multiple files
    val flightDF = if (args.length > 2 && args(2) == "full") {
      FlightUtils.loadMultipleData(
        spark,
        inputPath,
        sql.Encoders.product[FlightData].schema
      )
    } else {
      // Load just the csv file path passed in
      FlightUtils.loadData(
        spark,
        inputPath,
        sql.Encoders.product[FlightData].schema
      )
    }
    // Step 6. Load Carrier Summary files
    val carrierSummaryDF = FlightUtils.loadMultipleData(
      spark,
      carrierSummaryPath,
      sql.Encoders.product[CarrierSummaryData].schema
    )

    // Step 7: Select year to be used in the output file name and to improve processing speed
    val CarrierYears = carrierSummaryDF
      .select("YEAR")
      .distinct()
      .as[(Int)]
      .collect()

    // Step 8: Save Carrier Summary files to parquet
    CarrierYears.par.foreach { year =>
      FlightUtils.saveRawOutput(
        carrierSummaryDF,
        outputPath,
        "carrier_summary",
        s"carrier_summary_${year}"
      )
    }

    // Step 9: Repartition flight data DataFrame for better performance with Spark SQL
    logger.info("Repartitioning data")
    val partitionedFlightDataDF =
      flightDF.repartition(
        $"Year",
        $"Month",
        $"Origin",
        $"Dest",
        $"Flight_Number_Reporting_Airline"
      )
    logger.info("Done repartitioning data")

    // Step 10: Select year to be used in the output file name and to improve processing speed
    val years = partitionedFlightDataDF
      .select("Year")
      .distinct()
      .as[(Int)]
      .collect()

    // Step 11: Kick off the analysis portion
    logger.info("Running monthly analysis and saving results")

    // Step 12: Loop through each year to run our monthly analysis
    // Use par method to run years in paralel
    years.par.foreach { year =>
      // Step 13: Filter the data for the current month and year
      val filteredData = partitionedFlightDataDF
        .filter($"Year" === year)

      // Step 14: Create a temporary view for Spark SQL use
      filteredData.createOrReplaceTempView("flights")

      // Step 15: Run monthly flight analysis and save
      FlightData.runAnalysisAndSave(
        filteredData,
        spark,
        outputPath,
        "flight",
        MonthlyFlightAnalysisSchema.schema,
        s"monthly_flight_analysis_${year}",
        FlightData.monthlyFlightAnalysis
      )

      // Run monthly airport analysis and save
      FlightData.runAnalysisAndSave(
        filteredData,
        spark,
        outputPath,
        "airport",
        MonthlyAirportAnalysisSchema.schema,
        s"monthly_airport_analysis_${year}",
        FlightData.monthlyAirportAnalysis
      )

      // Run monthly airline analysis and save
      FlightData.runAnalysisAndSave(
        filteredData,
        spark,
        outputPath,
        "airline",
        MonthlyAirlineAnalysisSchema.schema,
        s"monthly_airline_analysis_${year}",
        FlightData.monthlyAirlineAnalysis
      )

      // Save monthly raw output
      val outputRawType = "raw"
      FlightUtils.saveRawOutput(
        filteredData,
        outputPath,
        outputRawType,
        s"monthly_raw_file_${year}"
      )
    }

    // Step 16: Notify of completion and stop the Spark session
    logger.info("All done!")
    spark.stop()
  }

}
