package io.github.mvamsichaitanya.codeconversion.sqltodataframe.utils

import net.sf.jsqlparser.statement.select.FromItem
import io.github.mvamsichaitanya.codeconversion.sqltodataframe._
import io.github.mvamsichaitanya.codeconversion.Config._
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.commons.lang3.math.NumberUtils.isNumber

/**
  * All util functions except parsing utils
  */

object CommonUtils {

  /**
    *
    * @param args      : Arguments
    * @param delimiter : delimiter separating arguments
    * @return Split arguments
    */
  def getArguments(args: String,
                   delimiter: Char): Array[String] = {
    var result = List[String]()
    val s = args.trim

    def go(i: Int): Unit = {
      val stack = scala.collection.mutable.Stack[Char]()
      var flag = 0
      var index = i
      val argument =
        for (c <- s.substring(i)
             if flag == 0) yield {
          index += 1
          if ((c == delimiter || c == ')') && stack.isEmpty) {
            flag = 1
            ' '
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
      result = argument.mkString.trim :: result
      if (index != s.length)
        go(index)
    }

    go(0)
    result.toArray.reverse
  }

  /**
    *
    * @param stmt :Statement
    * @return Removes braces if present
    */
  def getBetweenBraces(stmt: String): (String, Int) = {

    val s = stmt.trim
    val stack = scala.collection.mutable.Stack[Char]()
    var flag = 0
    val i = if (s.charAt(0) == '(') 1 else 0
    if (s.charAt(0) == '(') stack.push('(')
    var index = i
    val result = for (c <- s.substring(i); if flag == 0) yield {
      index += 1

      if ((c == ')' || c == ' ') && stack.isEmpty) {
        index = index - 1
        flag = 1
        ' '
      }
      else if (c == '(') {
        stack.push(c)
        c
      }
      else if (c == ')' && stack.top == '(') {
        stack.pop
        if (stack.isEmpty) ' ' else c
      }
      else
        c

    }
    (result.mkString.trim, index)
  }

  /**
    *
    * @param s string
    * @return true if `s` is a date
    */
  def isDate(s: String): Boolean = {

    val dateFormats = Array[SimpleDateFormat](
      new SimpleDateFormat("M/dd/yyyy"),
      new SimpleDateFormat("dd.M.yyyy"),
      new SimpleDateFormat("M/dd/yyyy hh:mm:ss a"),
      new SimpleDateFormat("dd.M.yyyy hh:mm:ss a"),
      new SimpleDateFormat("dd.MMM.yyyy"),
      new SimpleDateFormat("dd-MMM-yyyy"),
      new SimpleDateFormat("yyyy-MM-dd"))

    var result: Date = null
    dateFormats.foreach(dateFormat => {

      try {
        result = dateFormat.parse(s)
      }
      catch {
        case _: ParseException =>
        case e: Exception => throw new Exception(e)
      }
      if (result != null)
        return true
    })
    false
  }

  /**
    *
    * @param col Arithmetic equation
    * @return outer most arithmetic operator
    */
  def getOutermostOperator(col: String): Char = {


    arithmeticOperators.foreach(operator => {
      val args = getArguments(col, operator)
      if (args.length > 1)
        return operator
    })

    throw new Exception(s"failed to get outer most operator from $col")
  }

  /**
    *
    * @param col column
    * @return true if it is a date column
    */
  def isDateColumn(col: String): Boolean = {
    DateIdentifiers.foreach(identifier => {
      if (col.contains(identifier))
        return true
    })
    false
  }

  /**
    *
    * @param s String
    * @return table name
    */
  def getTable(s: String): String = if (s.contains('.')) s.split('.')(1) else s

  /**
    *
    * @param s String
    * @return true if s is numeric
    */
  def isNumeric(s: String): Boolean = isNumber(s.trim) ||
    (s.contains("lit(") && isNumeric(getBetweenBraces(s.split("lit").last)._1))

  /**
    *
    * @param s column
    * @return true if it contains arithmetic operator
    */
  def containArithmeticOperators(s: String): Boolean = {

    arithmeticOperators.foreach(op => {
      if (s.contains(op))
        return true
    })

    false
  }

  /**
    * Partially implemented Functions
    **/
  def isNested(s: FromItem): Boolean =
    if (s.toString.split(" ").length <= 2)
      false
    else
      true

  def isNested(s: String): Boolean =
    if (s.split(" ").length <= 2)
      false
    else
      true

  def isSql(s: String): Boolean =
    if (s.toUpperCase.contains("SELECT ") && s.toUpperCase.contains("FROM "))
      true
    else
      false

}
