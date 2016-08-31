import CustomImplicits._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object FlightsQueries extends App {
  val conf = new SparkConf().setAppName("BytesCount")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)

  val airports = sql.csvFile("/training/hive/airports/airports.csv", cacheTable = true)
  val carriers = sql.csvFile("/training/hive/carriers/carriers.csv", cacheTable = true)
  val flights = sql.csvFile("/training/hive/flights/2007.csv.bz2", cacheTable = true)

  // query1
  val flightsPerCarrier = flights
      .groupBy("UniqueCarrier")
      .count()
      .withColumnRenamed("UniqueCarrier", "Code")
      .join(carriers, "Code")
      .withColumnRenamed("Description", "carrier")
      .select("carrier", "count")
      .orderBy(column("count").desc)

  // query 2
  val newYorkAirports = airports
      .filter(lower(column("city")) === "new york")
      .select("iata")
      .collect()
      .map(_.getString(0))

  val flightsServedByNY = flights
      .filter(column("Month") === 6)
      .filter(column("Origin").isin(newYorkAirports:_*)
          .or(column("Dest").isin(newYorkAirports:_*)))

  // query 3
  val summerFlights = flights.filter(column("Month").between(6, 8))
  summerFlights.cache()
  val mostBusyAirports = summerFlights
      .select("Origin")
      .unionAll(summerFlights.select("Dest"))
      .withColumnRenamed("Origin", "iata")
      .groupBy("iata")
      .count()
      .orderBy(column("count").desc)
      .limit(5)
      .join(airports, "iata")
      .select("airport", "count")
      .orderBy(column("count").desc)
  summerFlights.unpersist()
}