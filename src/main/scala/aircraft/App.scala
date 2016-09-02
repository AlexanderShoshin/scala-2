package aircraft

object App extends App {
  val cliParser = new cliParser
  cliParser.parse(args, DataLocation()) match {
    case Some(dataLocation) => Queries.execute(dataLocation)
    case None => println("Restart the application with correct arguments")
  }
}