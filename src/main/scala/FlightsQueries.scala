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

  val flightsPerCarrier = flights
      .groupBy("UniqueCarrier")
      .count()
      .withColumnRenamed("UniqueCarrier", "Code")
      .join(carriers, "Code")
      .withColumnRenamed("Description", "carrier")
      .select("carrier", "count")
      .orderBy(column("count").desc)

  val newYorkAirports = airports
      .filter(lower(column("city")) === "new york")
      .select("iata")
      .collect()
      .map(_.getString(0))

  val flightsServedByNY = flights
      .filter(column("Month") === 6)
      .filter(column("Origin").isin(newYorkAirports:_*)
          .or(column("Dest").isin(newYorkAirports:_*)))

  
}