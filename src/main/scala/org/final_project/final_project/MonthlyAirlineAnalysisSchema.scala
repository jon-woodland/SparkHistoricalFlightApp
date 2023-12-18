package org.final_project.final_project

import org.apache.spark.sql.types._

/** This object defines the schema for the result of MonthlyAirlineAnalysis.
  */
object MonthlyAirlineAnalysisSchema {
  val schema: StructType = StructType(
    Seq(
      StructField("Reporting_Airline", StringType),
      StructField("Year", IntegerType),
      StructField("Month", IntegerType),
      StructField("flight_count", LongType),
      StructField("on_time_flights", LongType),
      StructField("cancelled_flights", LongType),
      StructField("diverted_flights", LongType),
      StructField("on_time_flight_percentage", DoubleType),
      StructField("cancelled_flight_percentage", DoubleType),
      StructField("diverted_flight_percentage", DoubleType),
      StructField("taxi_out_avg", DoubleType),
      StructField("taxi_in_avg", DoubleType),
      StructField("air_time_avg", DoubleType),
      StructField("origin_airport_count", LongType),
      StructField("dest_airport_count", LongType),
      StructField("departure_delay_avg", DoubleType),
      StructField("arrival_delay_avg", DoubleType),
      StructField("sum_arr_delay_min", DoubleType),
      StructField("sum_dep_delay_min", DoubleType),
      StructField("carrier_delay_count", LongType),
      StructField("sum_carrier_delay_min", DoubleType),
      StructField("weather_delay_count", LongType),
      StructField("sum_weather_delay_min", DoubleType),
      StructField("NAS_delay_count", LongType),
      StructField("sum_NAS_delay_min", DoubleType),
      StructField("security_delay_count", LongType),
      StructField("sum_security_delay_min", DoubleType),
      StructField("late_aircraft_delay_count", LongType),
      StructField("sum_late_aircraft_delay_min", DoubleType),
      StructField("reliability", StringType)
    )
  )
}
