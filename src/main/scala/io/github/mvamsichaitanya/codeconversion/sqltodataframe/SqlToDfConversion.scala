package io.github.mvamsichaitanya.codeconversion.sqltodataframe

import java.io.FileWriter
import io.github.mvamsichaitanya.codeconversion.CodeConversion
import io.github.mvamsichaitanya.codeconversion.Config._
import org.apache.commons.lang3.StringUtils
import io.github.mvamsichaitanya.codeconversion.sqltodataframe.constants.StringConstants._
import io.github.mvamsichaitanya.codeconversion.sqltodataframe.utils.CommonUtils._
import io.github.mvamsichaitanya.codeconversion.sqltodataframe.utils.ParsingUtils._

import scala.io.Source

object SqlToDfConversion extends CodeConversion {

  /**
    * tablesLoaded : Keep track of tables that are loaded
    * variables : Keep track of variables(String interpolated) found during parsing
    * varDataSets : Keep track of DataSets created fro Inset into Sql statements
    * fw : FileWriter object to write converted code to the output file
    */

  var tablesLoaded: List[String] = Nil
  var variables: List[String] = TranslateRegex :: Nil
  var varDataSets: List[String] = Nil
  var dateVars: Map[String, String] = Map.empty
  var fw: FileWriter = _

  /**
    *
    * @param stmt : spark sql statement
    * @return : Gets Sql statement present between spark.sql
    */
  def getSql(stmt: String): String =
    stmt.
      replace("spark.sql(s", "").
      replace(""""")""", "").replace('"', ' ')

  def isSqlCompleted(line: String): Boolean =
    line.trim.contains(""""")""")

  /**
    * Entry point for starting code conversion
    */
  override def start(): Unit = {

    /*
    Initializing scala file
      */
    val initial = DfFileInitialization
    val filename = DfInputFilePath
    val target = DfOutputFilePath
    /*
    Creating empty file
      */
    val f = new FileWriter(target)
    f.write(EmptyString)
    f.close()

    fw = new FileWriter(target, true)
    fw.write(initial)
    /*
    Read each sql statement to this codeSnippet variable and
    convert spark sql to spark DataFrame
    */
    var codeSnippet = EmptyString
    var tableName = EmptyString

    /*
    sqlStmtType holds the type of statement we are processing
    it is initialized as anonymous statement
    values it can hold is (AnonymousStmt, CreateStmt, InsertIntoStmt)
    */
    var sqlStmtType: SqlStmtType = AnonymousStmt

    /*
       Created each spark sql statement such that it ends with """)
    */
    Source.fromFile(filename).getLines.foreach { line => {

      sqlStmtType match {

        case CreateStmt if isSqlCompleted(line) =>

          codeSnippet = codeSnippet + line + "\n"
          val dataFrame = new DataFrame(
            getSql(codeSnippet),
            tableName,
            CreateStmt)
          codeSnippet = EmptyString
          tableName = EmptyString
          sqlStmtType = AnonymousStmt

        case InsertIntoStmt if isSqlCompleted(line) =>

          codeSnippet = codeSnippet + line + "\n"
          val sql = getSql(codeSnippet)
          tableName = getInsertIntoTable(sql)
          val selectStmt = getInsertIntoStmt(sql)
          if (!varDataSets.contains(tableName)) {
            varDataSets = tableName :: varDataSets
            fw.write(s"var $tableName:DataFrame = _ \n")
          }
          val dataFrame =
            new DataFrame(selectStmt, tableName, InsertIntoStmt)
          codeSnippet = EmptyString
          tableName = EmptyString
          sqlStmtType = AnonymousStmt

        case CreateStmt | InsertIntoStmt => codeSnippet += line + "\n"

        case AnonymousStmt if line.contains("=") && line.contains("val ") =>

          val stmt = line.toUpperCase.replaceAll("\\s", EmptyString)
          val l = stmt.length
          tableName = stmt.substring(3, l - 1)
          sqlStmtType = CreateStmt

        case AnonymousStmt if line.contains("""spark.sql(""") =>

          sqlStmtType = InsertIntoStmt
          codeSnippet += line

        case _ =>

      }
    }
    }
    fw.write("}}")
    fw.close()
  }

  /**
    * Util Functions written here because they use [[variables]]
    **/

  /**
    * @param column : Possible values Column name or user defined variable or variable with ' '
    * @return : true if column is variable
    */
  def isVariable(column: String): Boolean = {
    val col = column.trim
    if (isDate(col))
      true
    else if (variables.contains(col))
      true
    else if (StringUtils.isNumeric(col))
      true
    else if (col.charAt(0) == ''' && col.last == ''')
      true
    else
      false
  }

  /**
    * Converts passes column into variable
    */
  def convertToVariable(column: String): String = {
    val col = column.trim
    if (isDate(col)) {
      if (dateVars.getOrElse(col, null) == null) {
        dateVars = dateVars + (col -> s"date${dateVars.size}")
        fw.write(s"""val date${dateVars.size} = $col""")
      }

      "lit(" + dateVars(col) + ")"
    }
    else if (variables.contains(col.trim.replace("'", "")) || isNumeric(col))
      "lit(" + col + ")"
    else
      "lit(" + col.replace(''', '"') + ")"
  }


  def main(args: Array[String]): Unit = {
    val inputSql =
      """select  count(trim(concat(translate(student_id,'^%&*$',''),'_',name))) as new_id
          from STUDENT
          where student_id is not null
        """.stripMargin


    val dataFrame = new DataFrame(
      inputSql,
      "df",
      CreateStmt)

    println(dataFrame)



  }


}