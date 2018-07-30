package io.github.mvamsichaitanya.codeconversion.sastosparksql

import java.io.FileWriter

import io.github.mvamsichaitanya.codeconversion.CodeConversion
import io.github.mvamsichaitanya.codeconversion.sqltodataframe.constants.StringConstants._
import io.github.mvamsichaitanya.codeconversion.Config._
import scala.io.Source

/**
  * Driver program to convert sas to spark sql
  */

object SasToSparkSqlConversion extends CodeConversion {

  /**
    *
    */
  override def start(): Unit = {

    //Initializing scala file

    val initial = SqlFileInitialization
    val filename = InputFilePath
    val target = SqlOutputFilePath

    // Creating empty file
    val f = new FileWriter(target)
    f.write(EmptyString)
    f.close()

    val fw = new FileWriter(target, true)
    fw.write(initial)
    /*
     Read each sql statement to this codeSnippet variable and
      convert com.codeconversion.sas sql to spark sql
     */
    var codeSnippet = EmptyString
    var tableName = EmptyString

    // Flag is used to indicate the starting of fresh sql statement

    var sqlStmtType: SqlStmtType = AnonymousStmt
    /*
    Read each line in the file until ';' ,
    In com.codeconversion.sas  sql statements are separated by ';'
     */
    for (line <- Source.fromFile(filename).getLines) {


      sqlStmtType match {

        case InsertIntoStmt | CreateStmt if line.contains(";") &&
          !line.toUpperCase.contains("TRANSLATE(") =>

          if (!line.replaceAll("\\s", EmptyString).
            toUpperCase.contains("DISTRIBUTEON"))
            codeSnippet = codeSnippet + line.replace(EndSqlInd, "") + "\n"

          //Processing of sql statement starts
          val sparkSql = new SparkSql(codeSnippet, tableName, fw)
          sparkSql.create()

          codeSnippet = EmptyString
          tableName = EmptyString
          sqlStmtType = AnonymousStmt

        case InsertIntoStmt | CreateStmt => codeSnippet = codeSnippet + line + "\n"

        case AnonymousStmt if line.toUpperCase.contains("CREATE ") &&
          line.toUpperCase.contains("TABLE") =>

          sqlStmtType = CreateStmt
          val stmt = line.toUpperCase.replaceAll("\\s", EmptyString)
          val l = stmt.length

          val tableNmIndex =
            if (stmt.contains("CREATETEMPTABLE"))
              15
            else
              11

          val table = stmt.substring(tableNmIndex, l - 2)
          tableName = table.replaceAll("&", EmptyString)

        case AnonymousStmt if line.toUpperCase.contains("INSERT ") &&
          line.toUpperCase.contains("INTO") =>

          codeSnippet += line + "\n"
          tableName = EmptyString
          sqlStmtType = InsertIntoStmt

        case _ =>
      }

    }
    fw.write("}}")
    fw.close()
  }

}
