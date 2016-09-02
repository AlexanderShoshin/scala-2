package aircraft

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.FlatSpec

/**
  * Created by Alexander_Shoshin on 9/2/2016.
  */
class FlightsQueriesTest extends FlatSpec with SharedSparkContext {
  var flights: DataFrame = _
  var carriers: DataFrame = _
  var airports: DataFrame = _

  override def beforeAll() = {
    super.beforeAll()
    val sql = new SQLContext(sc)
    flights = generateTestFlights(sql)
    carriers = generateTestCarriers(sql)
    airports = generateTestAirports(sql)
  }

  def generateTestFlights(sql: SQLContext) = {
    val flightsData = sc.parallelize(List(
      "1,id_1,SVO,BBD",
      "2,id_1,DME,SVO",
      "2,id_2,BBD,SVO"
    ))
        .map(_.split(","))
        .map(flight => Flight(flight(0).toInt, flight(1), flight(2), flight(3)))
    sql.createDataFrame(flightsData)
  }

  def generateTestCarriers(sql: SQLContext) = {
    val carriersData = sc.parallelize(List(
      "id_1,Carrier 1",
      "id_2,Carrier 2"
    ))
        .map(_.split(","))
        .map(carrier => Carrier(carrier(0), carrier(1)))
    sql.createDataFrame(carriersData)
  }

  def generateTestAirports(sql: SQLContext) = {
    val airportsData = sc.parallelize(List(
      "SVO,Sheremetevo,Moscow",
      "DME,Domodedovo,Moscow",
      "BBD,Curtis,Brady"
    ))
        .map(_.split(","))
        .map(airport => Airport(airport(0), airport(1), airport(2)))
    sql.createDataFrame(airportsData)
  }

  it should "count carriers flights" in {
    val carrierFlights = Statistics.countCarriersFlights(flights, carriers).collect()
    val expected = List(Row("Carrier 1", 2), Row("Carrier 2", 1))
    assertResult(expected)(carrierFlights)
  }

  it should "count city flights in chosen month" in {
    val carrierFlights = Statistics.countFlightsByCity(flights, airports, "Moscow", month = 2).collect()
    val expected = List(Row(2))
    assertResult(expected)(carrierFlights)
  }

  it should "find most busy airport" in {
    val mostBusyAirport = Statistics.findMostBusyAirports(flights, airports, count = 1, Array(1, 2)).collect()
    val expected = List(Row("Sheremetevo", 3))
    assertResult(expected)(mostBusyAirport)
  }

  it should "find most busy carrier" in {
    val mostBusyCarrier = Statistics.findMostBusyCarriers(flights, carriers, count = 1).collect()
    val expected = List(Row("Carrier 1", 2))
    assertResult(expected)(mostBusyCarrier)
  }
}
