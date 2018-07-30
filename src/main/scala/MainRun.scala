
import io.github.mvamsichaitanya.codeconversion.sastosparksql.SasToSparkSqlConversion
import io.github.mvamsichaitanya.codeconversion.sqltodataframe.SqlToDfConversion

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
