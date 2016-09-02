package aircraft

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Statistics {
  def countCarriersFlights(flights: DataFrame, carriers: DataFrame) = {
    flights
        .groupBy("UniqueCarrier")
        .count()
        .withColumnRenamed("UniqueCarrier", "Code")
        .join(carriers, "Code")
        .withColumnRenamed("Description", "carrier")
        .select("carrier", "count")
        .orderBy(column("count").desc)
  }

  def countFlightsByCity(flights: DataFrame, airports: DataFrame, city: String, month: Int) = {
    val newYorkAirports = airports
        .filter(lower(column("city")) === city.toLowerCase)
        .select("iata")
        .collect()
        .map(_.getString(0))

    flights
        .filter(column("Month") === month)
        .filter(column("Origin").isin(newYorkAirports:_*)
            .or(column("Dest").isin(newYorkAirports:_*)))
        .groupBy()
        .count()
  }

  def findMostBusyAirports(flights: DataFrame, airports: DataFrame, count: Int, months: Array[Int]) = {
    val summerFlights = flights.filter(column("Month").isin(months:_*))
    summerFlights
        .select("Origin")
        .unionAll(summerFlights.select("Dest"))
        .withColumnRenamed("Origin", "iata")
        .groupBy("iata")
        .count()
        .orderBy(column("count").desc)
        .limit(count)
        .join(airports, "iata")
        .select("airport", "count")
        .orderBy(column("count").desc)
  }

  def findMostBusyCarriers(flights: DataFrame, carriers: DataFrame, count: Int) = {
    flights
        .groupBy("UniqueCarrier")
        .count()
        .withColumnRenamed("UniqueCarrier", "Code")
        .orderBy(column("count").desc)
        .limit(count)
        .join(carriers, "Code")
        .withColumnRenamed("Description", "carrier")
        .select("carrier", "count")
  }
}