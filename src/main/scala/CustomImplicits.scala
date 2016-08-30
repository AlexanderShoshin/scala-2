import org.apache.spark.sql.SQLContext

object CustomImplicits {
  implicit class CustomSQLContext(sql: SQLContext) {
    def csvFile(path: String, cacheTable: Boolean = false) = {
      val table = sql
          .read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("escape", "\\")
          .load(path)
      if (cacheTable) table.cache()
      table
    }
  }
}