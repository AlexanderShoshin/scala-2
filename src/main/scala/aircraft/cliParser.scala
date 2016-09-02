package aircraft

class cliParser extends scopt.OptionParser[DataLocation]("scala-2") {
  opt[String]('f', "flights")
      .required()
      .valueName("<file>")
      .action( (value, conf) => conf.copy(flightsPath = value) )
      .text("path to flights dataset file")

  opt[String]('c', "carriers")
      .required()
      .valueName("<file>")
      .action( (value, conf) => conf.copy(carriersPath = value) )
      .text("path to carriers dataset file")

  opt[String]('a', "airports")
      .required()
      .valueName("<file>")
      .action( (value, conf) => conf.copy(airportsPath = value) )
      .text("path to airports dataset file")
}