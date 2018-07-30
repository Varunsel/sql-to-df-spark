package io.github.mvamsichaitanya.codeconversion

import io.github.mvamsichaitanya.codeconversion.sqltodataframe.constants.StringConstants.EmptyString
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import org.scalatest.{Matchers, WordSpec}
import scala.util.{Failure, Success, Try}

trait TestCodeBase extends WordSpec with Matchers {


  /**
    *
    * @param expectedSql  : Expected output
    * @param convertedSql : output
    * @return
    */
  def compareSql(expectedSql: String, convertedSql: String): Boolean = {

    validateSql(extractVariables(convertedSql))
    expectedSql.toUpperCase.replaceAll("\\s", EmptyString) ==
      convertedSql.toUpperCase.replaceAll("\\s", EmptyString)
  }

  /**
    *
    * @param sql : Sql to validate
    */
  def validateSql(sql: String): Unit = {
    Try {
      CCJSqlParserUtil.parse(sql)
    } match {
      case Success(_) =>
      case Failure(ex) => throw new Exception(s"Exception while parsing following sql \n $sql " +
        s" \n Cause of exception is \n ${ex.getCause}")
    }
  }

  /**
    *
    * @param expectedDf  : Expected DataFrame Code
    * @param convertedDf : Converted DataFrame Code
    * @return
    */
  def compareDataFrame(expectedDf: String, convertedDf: String): Boolean = {
    expectedDf.toUpperCase.replaceAll("\\s", EmptyString) ==
      convertedDf.toUpperCase.replaceAll("\\s", EmptyString)

  }

  /**
    * Removes String interpolation
    *
    * @param sql Sql
    * @return
    */
  def extractVariables(sql: String): String = {
    var stmt = sql
    sql.split('{').tail.foreach(x => {

      stmt = stmt.replace("'${" + x.split('}')(0) + "}'", x.split('}')(0))
      stmt = stmt.replace("${" + x.split('}')(0) + "}", x.split('}')(0))
    })
    stmt
  }
}
