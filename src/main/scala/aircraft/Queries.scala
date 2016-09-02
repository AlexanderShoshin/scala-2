package aircraft

import implicits.CustomImplicits.CustomSQLContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Queries {
  def execute(dataLocation: DataLocation) = {
    val conf = new SparkConf().setAppName("AircraftQueries")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)

    val airports = sql.csvFile(dataLocation.airportsPath, cacheTable = true)
    val carriers = sql.csvFile(dataLocation.carriersPath, cacheTable = true)
    val flights = sql.csvFile(dataLocation.flightsPath, cacheTable = true)

    // Count total number of flights per carrier in 2007
    val carriersFlights = Statistics.countCarriersFlights(flights, carriers)
    carriersFlights.show

    // The total number of flights served in Jun 2007 by NYC
    val newYorkFlightsCount = Statistics.countFlightsByCity(flights, airports, "New York", month = 6)
    newYorkFlightsCount.show

    // Find five most busy airports in US during Jun 01 - Aug 31
    val mostBusyAirports = Statistics.findMostBusyAirports(flights, airports, count = 5, months = Array(6, 7, 8))
    mostBusyAirports.show

    // Find the carrier who served the biggest number of flights
    val mostBusyCarrier = Statistics.findMostBusyCarriers(flights, carriers, count = 1)
    mostBusyCarrier.show
  }
}