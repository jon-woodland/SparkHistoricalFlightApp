package org.final_project.final_project

import scala.util.{Try}

final case class CarrierSummaryData(
    YEAR: Integer,
    QUARTER: Integer,
    AIRLINE_ID: Integer,
    UNIQUE_CARRIER: String,
    UNIQUE_CARRIER_NAME: String,
    UNIQUE_CARRIER_ENTITY: String,
    REGION: String,
    CARRIER: String,
    CARRIER_NAME: String,
    CARRIER_GROUP: String,
    CARRIER_GROUP_NEW: String,
    AIRPORT_TYPE: String,
    ORIGIN_AIRPORT_ID: String,
    ORIGIN_AIRPORT_SEQ_ID: String,
    ORIGIN_CITY_MARKET_ID: String,
    ORIGIN: String,
    ORIGIN_CITY_NAME: String,
    ORIGIN_COUNTRY_NAME: String,
    ORIGIN_STATE_NM: String,
    ORIGIN_STATE_FIPS: String,
    ORIGIN_STATE_ABR: String,
    ORIGIN_WAC: String,
    SERVICE_CLASS: String,
    REV_ACRFT_DEP_SCH_520: Double,
    REV_ACRFT_DEP_PERF_510: Double,
    REV_PAX_ENP_110: Double,
    REV_ENP_FREIGHT_217: Double,
    REV_ENP_MAIL_219: Double,
    NUM_MONTHS: Integer
)

object CarrierSummaryData {

  def apply(csvRow: String): Option[CarrierSummaryData] = Try {
    val fields = csvRow.split(",")
    CarrierSummaryData(
      YEAR = fields(0).toInt,
      QUARTER = fields(1).toInt,
      AIRLINE_ID = fields(2).toInt,
      UNIQUE_CARRIER = fields(3),
      UNIQUE_CARRIER_NAME = fields(4),
      UNIQUE_CARRIER_ENTITY = fields(5),
      REGION = fields(6),
      CARRIER = fields(7),
      CARRIER_NAME = fields(8),
      CARRIER_GROUP = fields(9),
      CARRIER_GROUP_NEW = fields(10),
      AIRPORT_TYPE = fields(11),
      ORIGIN_AIRPORT_ID = fields(12),
      ORIGIN_AIRPORT_SEQ_ID = fields(13),
      ORIGIN_CITY_MARKET_ID = fields(14),
      ORIGIN = fields(15),
      ORIGIN_CITY_NAME = fields(16),
      ORIGIN_COUNTRY_NAME = fields(17),
      ORIGIN_STATE_NM = fields(18),
      ORIGIN_STATE_FIPS = fields(19),
      ORIGIN_STATE_ABR = fields(20),
      ORIGIN_WAC = fields(21),
      SERVICE_CLASS = fields(22),
      REV_ACRFT_DEP_SCH_520 = fields(23).toDouble,
      REV_ACRFT_DEP_PERF_510 = fields(24).toDouble,
      REV_PAX_ENP_110 = fields(25).toDouble,
      REV_ENP_FREIGHT_217 = fields(26).toDouble,
      REV_ENP_MAIL_219 = fields(27).toDouble,
      NUM_MONTHS = fields(28).toInt
    )
  }.toOption
}
