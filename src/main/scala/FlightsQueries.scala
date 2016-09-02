import CustomImplicits._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object FlightsQueries extends App {
  val conf = new SparkConf().setAppName("BytesCount")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)

  val airports = sql.csvFile("/training/hive/airports/airports.csv", cacheTable = false)
  val carriers = sql.csvFile("/training/hive/carriers/carriers.csv", cacheTable = false)
  val flights = sql.csvFile("/training/hive/flights/2007.csv.bz2", cacheTable = false)

  // Count total number of flights per carrier in 2007
  val carriersFlights = AirStat.countCarriersFlights(flights, carriers)
  carriersFlights.show

  // The total number of flights served in Jun 2007 by NYC
  val newYorkFlightsCount = AirStat.countFlightsByCity(flights, airports, "New York", month = 6)
  newYorkFlightsCount.show

  // Find five most busy airports in US during Jun 01 - Aug 31
  val mostBusyAirports = AirStat.findMostBusyAirports(flights, airports, count = 5, months = Array(6, 7, 8))
  mostBusyAirports.show

  // Find the carrier who served the biggest number of flights
  val mostBusyCarrier = AirStat.findMostBusyCarriers(flights, carriers, count = 1)
  mostBusyCarrier.show
}