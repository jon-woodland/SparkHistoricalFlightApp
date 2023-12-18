package org.final_project.final_project

import org.apache.spark.sql.types._

/** This object defines the schema for the result of MonthlyFlightAnalysis.
  */
object MonthlyFlightAnalysisSchema {
  val schema: StructType = StructType(
    Seq(
      StructField("Flight_Number_Reporting_Airline", IntegerType),
      StructField("Reporting_Airline", StringType),
      StructField("Year", IntegerType),
      StructField("Month", IntegerType),
      StructField("Origin", StringType),
      StructField("Dest", StringType),
      StructField("flight_count", IntegerType),
      StructField("on_time_flights", IntegerType),
      StructField("cancelled_flights", IntegerType),
      StructField("diverted_flights", IntegerType),
      StructField("on_time_flight_percentage", DoubleType),
      StructField("cancelled_flight_percentage", DoubleType),
      StructField("diverted_flight_percentage", DoubleType),
      StructField("taxi_out_avg", DoubleType),
      StructField("taxi_in_avg", DoubleType),
      StructField("air_time_avg", DoubleType),
      StructField("departure_delay_avg", DoubleType),
      StructField("arrival_delay_avg", DoubleType),
      StructField("sum_arr_delay_min", IntegerType),
      StructField("sum_dep_delay_min", IntegerType),
      StructField("carrier_delay_count", IntegerType),
      StructField("sum_carrier_delay_min", IntegerType),
      StructField("weather_delay_count", IntegerType),
      StructField("sum_weather_delay_min", IntegerType),
      StructField("NAS_delay_count", IntegerType),
      StructField("sum_NAS_delay_min", IntegerType),
      StructField("security_delay_count", IntegerType),
      StructField("sum_security_delay_min", IntegerType),
      StructField("late_aircraft_delay_count", IntegerType),
      StructField("sum_late_aircraft_delay_min", IntegerType),
      StructField("reliability", StringType)
    )
  )
}
