
import io.github.mvamsichaitanya.codeconversion.sastosparksql.SasToSparkSqlConversion
import io.github.mvamsichaitanya.codeconversion.sqltodataframe.SqlToDfConversion

/**
  * Driver program to convert sql's to dataframe
  * Steps:-
  * 1) Converts SAS or Hive Sql to Spark Sql's
  * 2)Converts Spark SQL's to DataFrame's
  */
object MainRun {

  def main(args: Array[String]): Unit = {
    println("Converting SAS Sql to Spark SQL")
    SasToSparkSqlConversion.start()
    println("Conversion completed")

    println("Converting Spark SQL to Spark DataFrame")
    SqlToDfConversion.start()
    println("Conversion completed")
  }
}
