package org.final_project.final_project.flightData

import org.final_project.final_project.flightUtils.FlightUtils
import org.final_project.final_project.flightUtils.FlightUtils
import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.{LazyLogging}
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import scala.util.{Try}

/** Case class representing the schema for monthly on-time flight data. For more
  * information and complete column definitions please see
  * https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ
  */
final case class FlightData(
    Year: Integer,
    Quarter: String,
    Month: Integer,
    DayofMonth: String,
    DayOfWeek: String,
    FlightDate: String,
    Reporting_Airline: String,
    DOT_ID_Reporting_Airline: String,
    IATA_CODE_Reporting_Airline: String,
    Tail_Number: String,
    Flight_Number_Reporting_Airline: String,
    OriginAirportID: String,
    OriginAirportSeqID: String,
    OriginCityMarketID: String,
    Origin: String,
    OriginCityName: String,
    OriginState: String,
    OriginStateFips: String,
    OriginStateName: String,
    OriginWac: String,
    DestAirportID: String,
    DestAirportSeqID: String,
    DestCityMarketID: String,
    Dest: String,
    DestCityName: String,
    DestState: String,
    DestStateFips: String,
    DestStateName: String,
    DestWac: String,
    CRSDepTime: String,
    DepTime: String,
    DepDelay: String,
    DepDelayMinutes: Double,
    DepDel15: String,
    DepartureDelayGroups: String,
    DepTimeBlk: String,
    TaxiOut: String,
    WheelsOff: String,
    WheelsOn: String,
    TaxiIn: String,
    CRSArrTime: String,
    ArrTime: String,
    ArrDelay: String,
    ArrDelayMinutes: Double,
    ArrDel15: String,
    ArrivalDelayGroups: String,
    ArrTimeBlk: String,
    Cancelled: String,
    CancellationCode: String,
    Diverted: String,
    CRSElapsedTime: String,
    ActualElapsedTime: String,
    AirTime: String,
    Flights: String,
    Distance: String,
    DistanceGroup: String,
    CarrierDelay: Option[Double],
    WeatherDelay: Option[Double],
    NASDelay: Option[Double],
    SecurityDelay: Option[Double],
    LateAircraftDelay: Option[Double],
    FirstDepTime: String,
    TotalAddGTime: String,
    LongestAddGTime: String,
    DivAirportLandings: String,
    DivReachedDest: String,
    DivActualElapsedTime: String,
    DivArrDelay: String,
    DivDistance: String,
    Div1Airport: String,
    Div1AirportID: String,
    Div1AirportSeqID: String,
    Div1WheelsOn: String,
    Div1TotalGTime: String,
    Div1LongestGTime: String,
    Div1WheelsOff: String,
    Div1TailNum: String,
    Div2Airport: String,
    Div2AirportID: String,
    Div2AirportSeqID: String,
    Div2WheelsOn: String,
    Div2TotalGTime: String,
    Div2LongestGTime: String,
    Div2WheelsOff: String,
    Div2TailNum: String,
    Div3Airport: String,
    Div3AirportID: String,
    Div3AirportSeqID: String,
    Div3WheelsOn: String,
    Div3TotalGTime: String,
    Div3LongestGTime: String,
    Div3WheelsOff: String,
    Div3TailNum: String,
    Div4Airport: String,
    Div4AirportID: String,
    Div4AirportSeqID: String,
    Div4WheelsOn: String,
    Div4TotalGTime: String,
    Div4LongestGTime: String,
    Div4WheelsOff: String,
    Div4TailNum: String,
    Div5Airport: String,
    Div5AirportID: String,
    Div5AirportSeqID: String,
    Div5WheelsOn: String,
    Div5TotalGTime: String,
    Div5LongestGTime: String,
    Div5WheelsOff: String,
    Div5TailNum: String
)

/** Companion object to unpack csv files.
  */
object FlightData extends LazyLogging {

  /** apply method to create FlightData from a CSV row.
    * @param csvRow
    *   CSV row as a string
    * @return
    *   Option of FlightData. None if parsing fails.
    */
  def apply(csvRow: String): Option[FlightData] = Try {
    val fields = csvRow.split(",")
    FlightData(
      Year = fields(0).toInt,
      Quarter = fields(1),
      Month = fields(2).toInt,
      DayofMonth = fields(3),
      DayOfWeek = fields(4),
      FlightDate = fields(5),
      Reporting_Airline = fields(6),
      DOT_ID_Reporting_Airline = fields(7),
      IATA_CODE_Reporting_Airline = fields(8),
      Tail_Number = fields(9),
      Flight_Number_Reporting_Airline = fields(10),
      OriginAirportID = fields(11),
      OriginAirportSeqID = fields(12),
      OriginCityMarketID = fields(13),
      Origin = fields(14),
      OriginCityName = fields(15),
      OriginState = fields(16),
      OriginStateFips = fields(17),
      OriginStateName = fields(18),
      OriginWac = fields(19),
      DestAirportID = fields(20),
      DestAirportSeqID = fields(21),
      DestCityMarketID = fields(22),
      Dest = fields(23),
      DestCityName = fields(24),
      DestState = fields(25),
      DestStateFips = fields(26),
      DestStateName = fields(27),
      DestWac = fields(28),
      CRSDepTime = fields(29),
      DepTime = fields(30),
      DepDelay = fields(31),
      DepDelayMinutes = fields(32).toDouble,
      DepDel15 = fields(33),
      DepartureDelayGroups = fields(34),
      DepTimeBlk = fields(35),
      TaxiOut = fields(36),
      WheelsOff = fields(37),
      WheelsOn = fields(38),
      TaxiIn = fields(39),
      CRSArrTime = fields(40),
      ArrTime = fields(41),
      ArrDelay = fields(42),
      ArrDelayMinutes = fields(43).toDouble,
      ArrDel15 = fields(44),
      ArrivalDelayGroups = fields(45),
      ArrTimeBlk = fields(46),
      Cancelled = fields(47),
      CancellationCode = fields(48),
      Diverted = fields(49),
      CRSElapsedTime = fields(50),
      ActualElapsedTime = fields(51),
      AirTime = fields(52),
      Flights = fields(53),
      Distance = fields(54),
      DistanceGroup = fields(55),
      CarrierDelay = Option(fields(56))
        .filter(_.nonEmpty)
        .map(_.toDouble)
        .orElse(Some(0.00)),
      WeatherDelay = Option(fields(57))
        .filter(_.nonEmpty)
        .map(_.toDouble)
        .orElse(Some(0.00)),
      NASDelay = Option(fields(58))
        .filter(_.nonEmpty)
        .map(_.toDouble)
        .orElse(Some(0.00)),
      SecurityDelay = Option(fields(59))
        .filter(_.nonEmpty)
        .map(_.toDouble)
        .orElse(Some(0.00)),
      LateAircraftDelay = Option(fields(60))
        .filter(_.nonEmpty)
        .map(_.toDouble)
        .orElse(Some(0.00)),
      FirstDepTime = fields(61),
      TotalAddGTime = fields(62),
      LongestAddGTime = fields(63),
      DivAirportLandings = fields(64),
      DivReachedDest = fields(65),
      DivActualElapsedTime = fields(66),
      DivArrDelay = fields(67),
      DivDistance = fields(68),
      Div1Airport = fields(69),
      Div1AirportID = fields(70),
      Div1AirportSeqID = fields(71),
      Div1WheelsOn = fields(72),
      Div1TotalGTime = fields(73),
      Div1LongestGTime = fields(74),
      Div1WheelsOff = fields(75),
      Div1TailNum = fields(76),
      Div2Airport = fields(77),
      Div2AirportID = fields(78),
      Div2AirportSeqID = fields(79),
      Div2WheelsOn = fields(80),
      Div2TotalGTime = fields(81),
      Div2LongestGTime = fields(82),
      Div2WheelsOff = fields(83),
      Div2TailNum = fields(84),
      Div3Airport = fields(85),
      Div3AirportID = fields(86),
      Div3AirportSeqID = fields(87),
      Div3WheelsOn = fields(88),
      Div3TotalGTime = fields(89),
      Div3LongestGTime = fields(91),
      Div3WheelsOff = fields(92),
      Div3TailNum = fields(93),
      Div4Airport = fields(94),
      Div4AirportID = fields(95),
      Div4AirportSeqID = fields(96),
      Div4WheelsOn = fields(97),
      Div4TotalGTime = fields(98),
      Div4LongestGTime = fields(99),
      Div4WheelsOff = fields(100),
      Div4TailNum = fields(101),
      Div5Airport = fields(102),
      Div5AirportID = fields(103),
      Div5AirportSeqID = fields(104),
      Div5WheelsOn = fields(105),
      Div5TotalGTime = fields(106),
      Div5LongestGTime = fields(107),
      Div5WheelsOff = fields(108),
      Div5TailNum = fields(109)
    )
  }.toOption

  /** This method runs the analysis and saves the outputted DataFrame as a
    * Parquet file in S3.
    * @param df
    *   The Dataframe used
    * @param spark
    *   The Spark session
    * @param outputPath
    *   The output S3 path
    * @param outputType
    *   The type of the output
    * @param outputFileName
    *   The name of the output file
    * @param sqlQuery
    *   The spark sql query to be executed
    */
  def runAnalysisAndSave(
      df: DataFrame,
      spark: SparkSession,
      outputPath: String,
      outputType: String,
      outputSchema: StructType,
      outputFileName: String,
      sqlQuery: String
  ): Unit = {
    val resultDF = spark.sql(sqlQuery)
    val finalResultDF = computeReliability(resultDF)
    FlightUtils.saveTransformedOutput(
      finalResultDF,
      outputPath,
      outputType,
      outputSchema,
      outputFileName
    )
    logger.info(s"Done saving $outputFileName")
  }

  /** This method takes a DataFrame and computes the reliability.
    * @param df
    *   The Dataframe used
    */
  def computeReliability(df: DataFrame): DataFrame = {
    df.withColumn(
      "reliability",
      when(col("on_time_flight_percentage") > 90, "Very Reliable")
        .when(
          col("on_time_flight_percentage") <= 90 && col(
            "on_time_flight_percentage"
          ) > 80,
          "Somewhat Reliable"
        )
        .when(
          col("on_time_flight_percentage") <= 80 && col(
            "diverted_flight_percentage"
          ) > 10 && (col(
            "diverted_flight_percentage"
          ) > col("cancelled_flight_percentage")),
          "Likely to be Diverted"
        )
        .when(
          col("on_time_flight_percentage") <= 80 && col(
            "cancelled_flight_percentage"
          ) > 10 && (col(
            "cancelled_flight_percentage"
          ) > col("diverted_flight_percentage")),
          "Likely to be Cancelled"
        )
        .otherwise("Likely to be Delayed")
    )
  }

  /** MonthlyFlightAnalysis is a string value representing spark sql that will
    * be executed against the flights temp view to form the data and get basic
    * aggregation and insight into individual flights.
    * @return
    *   String sql statement
    */
  def monthlyFlightAnalysis: String = {
    val monFlightSQL =
      """
    | WITH cte AS (
    | SELECT
    |  Flight_Number_Reporting_Airline,
    |  Reporting_Airline,
    |  Year,
    |  Month,
    |  Origin,
    |  Dest,
    |  case when DepDelayMinutes <= 15 AND ArrDelayMinutes <= 15 THEN 1 ELSE 0 END AS on_time_flight,
    |  DepDelayMinutes,
    |  TaxiOut,
    |  TaxiIn,
    |  ArrDelayMinutes,
    |  Cancelled,
    |  Diverted,
    |  COALESCE(CarrierDelay,0) AS CarrierDelay,
    |  COALESCE(WeatherDelay,0) AS WeatherDelay,
    |  COALESCE(NASDelay,0) AS NASDelay,
    |  COALESCE(SecurityDelay,0) AS SecurityDelay,
    |  COALESCE(LateAircraftDelay,0) AS LateAircraftDelay,
    |  AirTime,
    |  Distance
    |FROM flights
    |)
    |  SELECT
    |  Flight_Number_Reporting_Airline,
    |  Reporting_Airline,
    |  Year,
    |  Month,
    |  Origin,
    |  Dest,
    |  count(*) AS flight_count,
    |  sum(on_time_flight) AS on_time_flights,
    |  sum(Cancelled) AS cancelled_flights,
    |  sum(Diverted) AS diverted_flights,
    |  (sum(on_time_flight)/count(*)) * 100 AS on_time_flight_percentage,
    |  (sum(Cancelled)/count(*)) * 100 AS cancelled_flight_percentage,
    |  (sum(Diverted)/count(*)) * 100 AS diverted_flight_percentage,
    |  avg(TaxiOut) AS taxi_out_avg,
    |  avg(TaxiIn) AS taxi_in_avg,
    |  avg(AirTime) AS air_time_avg,
    |  avg(case when DepDelayMinutes >= 15 THEN DepDelayMinutes END) AS departure_delay_avg,
    |  avg(case when ArrDelayMinutes >= 15 THEN ArrDelayMinutes END) AS arrival_delay_avg,
    |  sum(case when on_time_flight = 0 THEN ArrDelayMinutes ELSE 0 END) AS sum_arr_delay_min,
    |  sum(case when on_time_flight = 0 THEN DepDelayMinutes ELSE 0 END) AS sum_dep_delay_min,
    |  sum(case when CarrierDelay > 0 THEN 1 ELSE 0 END) AS carrier_delay_count,
    |  sum(CarrierDelay) AS sum_carrier_delay_min,
    |  sum(case when WeatherDelay > 0 THEN 1 ELSE 0 END) AS weather_delay_count,
    |  sum(WeatherDelay) AS sum_weather_delay_min,
    |  sum(case when NASDelay > 0 THEN 1 ELSE 0 END) AS NAS_delay_count,
    |  sum(NASDelay) AS sum_NAS_delay_min,
    |  sum(case when SecurityDelay > 0 THEN 1 ELSE 0 END) AS security_delay_count,
    |  sum(SecurityDelay) AS sum_security_delay_min,
    |  sum(case when LateAircraftDelay > 0 THEN 1 ELSE 0 END) AS late_aircraft_delay_count,
    |  sum(LateAircraftDelay) AS sum_late_aircraft_delay_min
    |FROM cte
    |GROUP BY Flight_Number_Reporting_Airline, Reporting_Airline,Year, Month, Origin, Dest""".stripMargin

    monFlightSQL
  }

  /** MonthlyAirportAnalysis is a string value representing spark sql that will
    * be executed against the flights temp view to form the data and get basic
    * aggregation and insight into individual airports.
    * @return
    *   String sql statement
    */
  def monthlyAirportAnalysis: String = {
    val monAirportSQL =
      """
    | WITH cte AS (
    | SELECT
    |  Flight_Number_Reporting_Airline,
    |  Reporting_Airline,
    |  Year,
    |  Month,
    |  Origin,
    |  Dest,
    |  case when DepDelayMinutes <= 15 AND ArrDelayMinutes <= 15 THEN 1 ELSE 0 END AS on_time_flight,
    |  DepDelayMinutes,
    |  TaxiOut,
    |  TaxiIn,
    |  ArrDelayMinutes,
    |  Cancelled,
    |  Diverted,
    |  COALESCE(CarrierDelay,0) AS CarrierDelay,
    |  COALESCE(WeatherDelay,0) AS WeatherDelay,
    |  COALESCE(NASDelay,0) AS NASDelay,
    |  COALESCE(SecurityDelay,0) AS SecurityDelay,
    |  COALESCE(LateAircraftDelay,0) AS LateAircraftDelay,
    |  AirTime,
    |  Distance
    |FROM flights
    | )   
    |SELECT
    |  Origin,
    |  Year,
    |  Month,
    |  count(*) AS flight_count,
    |  count(distinct Dest) AS destination_count,
    |  sum(on_time_flight) AS on_time_flights,
    |  sum(Cancelled) AS cancelled_flights,
    |  sum(Diverted) AS diverted_flights,
    |  (sum(on_time_flight)/count(*)) * 100 AS on_time_flight_percentage,
    |  (sum(Cancelled)/count(*)) * 100 AS cancelled_flight_percentage,
    |  (sum(Diverted)/count(*)) * 100 AS diverted_flight_percentage,
    |  avg(TaxiOut) AS taxi_out_avg,
    |  avg(TaxiIn) AS taxi_in_avg,
    |  avg(AirTime) AS air_time_avg,
    |  avg(case when DepDelayMinutes >= 15 THEN DepDelayMinutes END) AS departure_delay_avg,
    |  avg(case when ArrDelayMinutes >= 15 THEN ArrDelayMinutes END) AS arrival_delay_avg,
    |  sum(case when on_time_flight = 0 THEN ArrDelayMinutes ELSE 0 END) AS sum_arr_delay_min,
    |  sum(case when on_time_flight = 0 THEN DepDelayMinutes ELSE 0 END) AS sum_dep_delay_min,
    |  sum(case when CarrierDelay > 0 THEN 1 ELSE 0 END) AS carrier_delay_count,
    |  sum(CarrierDelay) AS sum_carrier_delay_min,
    |  sum(case when WeatherDelay > 0 THEN 1 ELSE 0 END) AS weather_delay_count,
    |  sum(WeatherDelay) AS sum_weather_delay_min,
    |  sum(case when NASDelay > 0 THEN 1 ELSE 0 END) AS NAS_delay_count,
    |  sum(NASDelay) AS sum_NAS_delay_min,
    |  sum(case when SecurityDelay > 0 THEN 1 ELSE 0 END) AS security_delay_count,
    |  sum(SecurityDelay) AS sum_security_delay_min,
    |  sum(case when LateAircraftDelay > 0 THEN 1 ELSE 0 END) AS late_aircraft_delay_count,
    |  sum(LateAircraftDelay) AS sum_late_aircraft_delay_min
    |FROM cte
    |GROUP BY Origin, Year, Month""".stripMargin

    monAirportSQL
  }

  /** MonthlyAirlineAnalysis is a string value representing spark sql that will
    * be executed against the flights temp view to form the data and get basic
    * aggregation and insight into individual airlines.
    * @return
    *   String sql statement
    */
  def monthlyAirlineAnalysis: String = {
    val monAirlineSQL =
      """
    | WITH cte AS (
    | SELECT
    |  Flight_Number_Reporting_Airline,
    |  Reporting_Airline,
    |  Year,
    |  Month,
    |  Origin,
    |  Dest,
    |  case when DepDelayMinutes <= 15 AND ArrDelayMinutes <= 15 THEN 1 ELSE 0 END AS on_time_flight,
    |  DepDelayMinutes,
    |  TaxiOut,
    |  TaxiIn,
    |  ArrDelayMinutes,
    |  Cancelled,
    |  Diverted,
    |  COALESCE(CarrierDelay,0) AS CarrierDelay,
    |  COALESCE(WeatherDelay,0) AS WeatherDelay,
    |  COALESCE(NASDelay,0) AS NASDelay,
    |  COALESCE(SecurityDelay,0) AS SecurityDelay,
    |  COALESCE(LateAircraftDelay,0) AS LateAircraftDelay,
    |  AirTime,
    |  Distance
    |FROM flights
    | )       
    |  SELECT
    |  Reporting_Airline,
    |  Year,
    |  Month,
    |  count(*) AS flight_count,
    |  sum(on_time_flight) AS on_time_flights,
    |  sum(Cancelled) AS cancelled_flights,
    |  sum(Diverted) AS diverted_flights,
    |  (sum(on_time_flight)/count(*)) * 100 AS on_time_flight_percentage,
    |  (sum(Cancelled)/count(*)) * 100 AS cancelled_flight_percentage,
    |  (sum(Diverted)/count(*)) * 100 AS diverted_flight_percentage,
    |  avg(TaxiOut) AS taxi_out_avg,
    |  avg(TaxiIn) AS taxi_in_avg,
    |  avg(AirTime) AS air_time_avg,
    |  count(DISTINCT Origin) AS origin_airport_count,
    |  count(DISTINCT Dest) AS dest_airport_count,
    |  avg(case when DepDelayMinutes >= 15 THEN DepDelayMinutes END) AS departure_delay_avg,
    |  avg(case when ArrDelayMinutes >= 15 THEN ArrDelayMinutes END) AS arrival_delay_avg,
    |  sum(case when on_time_flight = 0 THEN ArrDelayMinutes ELSE 0 END) AS sum_arr_delay_min,
    |  sum(case when on_time_flight = 0 THEN DepDelayMinutes ELSE 0 END) AS sum_dep_delay_min,
    |  sum(case when CarrierDelay > 0 THEN 1 ELSE 0 END) AS carrier_delay_count,
    |  sum(CarrierDelay) AS sum_carrier_delay_min,
    |  sum(case when WeatherDelay > 0 THEN 1 ELSE 0 END) AS weather_delay_count,
    |  sum(WeatherDelay) AS sum_weather_delay_min,
    |  sum(case when NASDelay > 0 THEN 1 ELSE 0 END) AS NAS_delay_count,
    |  sum(NASDelay) AS sum_NAS_delay_min,
    |  sum(case when SecurityDelay > 0 THEN 1 ELSE 0 END) AS security_delay_count,
    |  sum(SecurityDelay) AS sum_security_delay_min,
    |  sum(case when LateAircraftDelay > 0 THEN 1 ELSE 0 END) AS late_aircraft_delay_count,
    |  sum(LateAircraftDelay) AS sum_late_aircraft_delay_min
    |FROM cte
    |GROUP BY Reporting_Airline, Year, Month""".stripMargin

    monAirlineSQL
  }
}
