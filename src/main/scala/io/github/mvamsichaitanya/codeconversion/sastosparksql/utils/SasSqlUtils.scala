package io.github.mvamsichaitanya.codeconversion.sastosparksql.utils

import io.github.mvamsichaitanya.codeconversion.sastosparksql.SparkSql
import io.github.mvamsichaitanya.codeconversion.sastosparksql.constants.StringConstants.EndFunctionIndicators

object SasSqlUtils {


  /**
    * Function to find when Keyword of concat or cast function to be placed
    *
    * @param code       Code
    * @param function   Function which is to be placed at starting of code
    * @param delimiter  Delimiter Which separates Parameter
    * @param replaceVar Variable which is to be replace delimiter
    * @return Converted code
    */
  def startFunction(code: String,
                    function: String,
                    delimiter: String,
                    replaceVar: String,
                    funcSpecificEndIndicators: Seq[Char] = Nil): String = {

    def reverse(s: String): String = {

      val word = s.reverse

      val result = for (w <- word) yield {
        if (w == ')')
          '('
        else if (w == '(')
          ')'
        else
          w
      }

      result.toString
    }

    val lines = code.split("\n").reverse
    val s = scala.collection.mutable.Stack[Char]()
    var flag = 0
    var startBraceInd = 0
    var startLineInd = 0

    val result =
      for (line <- lines
           if (s.nonEmpty || line == lines(0)) &&
             flag == 0) yield {
        startLineInd += 1
        val rev = reverse(line)
        val chars = rev.toCharArray
        startBraceInd = 0

        var isReachedEndOfLine: Boolean = false

        val l = line.length
        val concat =
          for (c <- chars
               if flag == 0) yield {
            startBraceInd += 1

            if (EndFunctionIndicators.contains(c) &&
              s.isEmpty &&
              (startBraceInd != 1)) {

              startBraceInd -= 1
              flag = 1
              c
            }
            else if (c == ')' &&
              s.nonEmpty &&
              s.top == '(') {
              if (startBraceInd == l) {
                isReachedEndOfLine = true
                flag = 1
              }

              s.pop
              c
            }
            else if (c == ''' &&
              s.nonEmpty &&
              s.top == ''') {

              if (startBraceInd == l) {
                isReachedEndOfLine = true
                flag = 1
              }
              s.pop

            } else if (c == '(' || c == ''') {
              s.push(c)
              c
            }
            else if (startBraceInd == l &&
              s.isEmpty) {
              isReachedEndOfLine = true
              flag = 1
              c
            }
            else
              c
          }



        //        println(containsWholeLine)
        //        println(reverse(rev.substring(startBraceInd))+" txthgcghchg")
        //        println("vamsi "+reverse(concat.init.mkString.trim))
        if (flag == 1)
          reverse(rev.substring(startBraceInd)) +
            //            reverse(concat.last.toString) +
            s" \n $function(" +
            reverse(if (isReachedEndOfLine) concat.mkString.trim
            else concat.init.mkString.trim).
              replace(delimiter, ";").replace(";", replaceVar)
        else
          reverse(concat.mkString.trim).
            replace(delimiter, ";").
            replace(";", replaceVar)
      }

    lines.drop(startLineInd).reverse.mkString("\n") +
      result.reverse.mkString("\n")
  }

  /**
    * Function to find when braces of concat or cast function to be closed
    *
    * @param code         Code
    * @param delimiter    Delimiter Which separates Parameter
    * @param replaceVar   Variable which is to be replace delimiter
    * @param closingBrace Braces at the end of function
    * @return Returns converted code
    */
  def closeFunction(code: String,
                    delimiter: String,
                    replaceVar: String,
                    closingBrace: String,
                    funcSpecificEndIndicators: Seq[Char] = Nil): String = {

    //    println("nnkn "+code)

    var closingBraceInd = 0
    val lines = code.trim.split("\n")
    val stack = scala.collection.mutable.Stack[Char]()
    var flag = 0
    var endLineInd = 0
    val result =
      for (line <- lines
           if (stack.nonEmpty || line == lines(0)) ||
             ((stack.nonEmpty || line != lines(0)) && flag == 0)) yield {

        endLineInd += 1
        val chars = line.toCharArray
        closingBraceInd = 0

        val concat =
          for (c <- chars
               if flag == 0) yield {
            closingBraceInd += 1
            if ( (EndFunctionIndicators ++ funcSpecificEndIndicators).contains(c) &&
              stack.isEmpty &&
              (closingBraceInd != 1 || endLineInd > 1)) {
              flag = 1
              c
            }
            else if (c == ')' &&
              stack.nonEmpty &&
              stack.top == '(') {
              stack.pop
              c
            }
            else if (c == ''' &&
              stack.nonEmpty &&
              stack.top == ''')
              stack.pop

            else if (c == '(' || c == ''') {
              stack.push(c)
              c
            }
            else
              c
          }

        if (flag == 1)
          concat.init.
            mkString.trim.
            replace(delimiter, ";").
            replace(";", replaceVar) +
            closingBrace +
            concat.last +
            line.substring(closingBraceInd)
        else
          concat.mkString.
            trim.replace(delimiter, ";").
            replace(";", replaceVar)
      }

    if (flag == 0)
      result.mkString("\n").replace(delimiter, ";").replace(";", ",") + closingBrace +
        lines.drop(endLineInd).mkString("\n")
    else
      result.mkString("\n") +
        lines.drop(endLineInd).mkString("\n")
  }

  def main(args: Array[String]): Unit = {

    val inputSasSqlStmt =
      """SELECT
                          	DISTINCT student_id AS id,
                               (branch||'_'||%str(%')&postfix%str(%')) :: char(64) AS branch_college
                          FROM
                          	STUDENT"""
    val sparkSql = new SparkSql(inputSasSqlStmt, "", null)
    sparkSql.create()
    val convertedSql = sparkSql.convertedSql
    println(convertedSql)
  }
}
