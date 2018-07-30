package io.github.mvamsichaitanya.codeconversion.sastosparksql

// todo: implement encde and decde

import io.github.mvamsichaitanya.codeconversion.sastosparksql.utils.SasSqlUtils._
import io.github.mvamsichaitanya.codeconversion.sastosparksql.constants.StringConstants._
import java.io._

class SparkSql(sqlStmt: String,
               tableName: String,
               fw: FileWriter = null) {

  var convertedSql: String = _

  def create(): Unit = {

    val sql = sqlStmt.replace("HASH8", "HASH").replace("CHARACTER", "CHAR").
      replace("hash8", "hash").replace("character", "char")
    val lines = sql.toLowerCase().split("\n")

    val result = for (line <- lines) yield {

      val translateLine = process(processTranslate)(line, "TRANSLATE(")
      val extractLine = process(processExtract)(translateLine, "%STR(%")
      val varLine = process(processVariable)(extractLine, "&")
      val randLine = process(processRandom)(varLine, "RANDOM()")
      val tableLine = process(processTable)(randLine, "..")
      val nvl = process(processNvl)(tableLine, "NVL")

      nvl
    }

    /*
    Concat and cast functions may extend to multiple lines
     So converting them by passing whole sql statement
   */
    val concatStatement = processConcat(result.mkString("\n"))
    val castStatement = processCast(concatStatement)

    convertedSql = castStatement
    if (fw != null)
      write(castStatement)
  }

  /*
   Higher order function which takes function  and
   identifier as input
  */
  def process(f: String => String)
             (line: String,
              identifier: String): String =
    if (line.toUpperCase.contains(identifier))
      f(line)
    else
      line

  def processNvl(line: String): String =
    line.replace("nvl", "coalesce").replace("NVL", "coalesce")

  def processTranslate(line: String): String = {

    val arr = line.split("translate")
    val stack = scala.collection.mutable.Stack[Char]()
    var flag = 0
    var index = 0
    val result = for (c <- arr(1); if flag == 0) yield {
      index += 1
      if (c == ')' && stack.isEmpty) {
        flag = 1
        c
      }
      else if (c == '(') {
        stack.push(c)
        c
      }
      else if (c == ')' && stack.top == '(') {
        stack.pop
        c
      }
      else
        c
    }
    val translateElements = result.mkString.split(",'")
    translateElements.length match {
      case 3 => translateElements(1) = TranslateRegex
      case _ => throw new Exception(TranslateFunctionException)
    }

    arr(0) + "translate" +
      translateElements(0) + "," +
      translateElements.tail.mkString(",'") + arr(1).substring(index)
  }

  //Changing table and schema names

  def processTable(line: String): String = {
    line.replace("..", ";").split(';').mkString(".")
  }

  def processRandom(line: String): String =
    line.replace("RANDOM()", "rand()").replace("random()", "rand()")

  //Converting com.codeconversion.sas variable to String interpolated variable in scala

  def processVariable(l: String): String = {

    def getVariable(line: String,
                    delimiter: String): String = {
      val words = line.trim.split(delimiter)

      val result = for (w <- words) yield {
        if (w.contains('&')) {
          if (w.contains('-'))
            getVariable(w, "-")
          else if (w.contains("="))
            getVariable(w, "=")
          else if (w.contains('.'))
            getVariable(w.replace('.', ';'), ";").replace(';', '.')
          else {
            val arr = w.split('&')
            arr(0) + "${" + closeFunction(arr(1), " ", ";", "}",Seq(''')) //arr(1) + "}"
          }
        }
        else
          w
      }

      result.mkString(delimiter)
    }

    getVariable(l, " ")
  }

  def processExtract(line: String): String =
    line.replace("%str(%')", "'")


  //Converts a||b||c to CONCAT(a,b,c)

  def processConcat(sqlStmt: String): String = {
    if (sqlStmt.contains("||")) {
      val lines = sqlStmt.split("\n")

      val firstOccurrence = lines.
        zipWithIndex.
        filter(line => line._1.contains("||")).
        map(tuple => tuple._2).head

      val len = lines(firstOccurrence).
        replace("||", ";").split(";").length

      val count = lines(firstOccurrence).
        replace("||", ";").filter(x => x == ';').length

      val mid =
        if (len == count)
          ";"
        else
          ""

      val first = lines.
        dropRight(lines.length - firstOccurrence).
        mkString("\n") + "\n" +
        lines(firstOccurrence).replace("||", ";").split(";").head.trim

      val last = lines(firstOccurrence).
        replace("||", ";").
        split(";").
        tail.mkString("||") + mid + "\n" +
        lines.reverse.dropRight(firstOccurrence + 1).reverse.mkString("\n")

//      val endOfFunction = last.split(" as ").head.replace("||", ";").replace(";",",") +
//        ")" + " as " + last.split(" as ").tail.mkString(" as ")

      processConcat(startFunction(first, "CONCAT", "||", ",") + "," + closeFunction(last,"||",",",")"))
    }
    else
      sqlStmt
  }


  //Converting a :: int to CAST(a,int)

  def processCast(sqlStmt: String): String = {

//    println(sqlStmt)
    if (sqlStmt.contains("::")) {
      val lines = sqlStmt.split("\n")

      val firstOccurence = lines.
        zipWithIndex.
        filter(line => line._1.contains("::")).
        map(tuple => tuple._2).head

      val first = lines.
        dropRight(lines.length - firstOccurence).
        mkString("\n") + "\n " +
        lines(firstOccurence).replace("::", ";").split(";").head.trim

      val last = lines(firstOccurence).
        replace("::", ";").
        split(";").
        tail.mkString("::") + " " + "\n" +
        lines.reverse.dropRight(firstOccurence + 1).reverse.mkString("\n")


      processCast(startFunction(first, "CAST", "::", ",") +
        " as " + closeFunction(last + " ", "::", ",", " ) "))

    }
    else
      sqlStmt

  }


  //writing each statements as data frame to  the file and creating temporary view on it

  def write(sqlStmt: String): Unit = {

    convertedSql = sqlStmt

    val view =
      s"""
         $tableName.createOrReplaceTempView("$tableName")
         """

    val stmt =s"""$sqlStmt"""
    val valStmt =
      s"""
         |val $tableName =
    """.stripMargin

    if (tableName != "") fw.write(valStmt)

    fw.write("spark.sql(")
    fw.write("s" + TripleQuote)
    fw.write(stmt)
    fw.write(TripleQuote)
    fw.write(")" + "\n")

    if (tableName != "")
      fw.write("\n" + view + "\n")

  }

  override def toString: String = tableName + "\n" + convertedSql

}
