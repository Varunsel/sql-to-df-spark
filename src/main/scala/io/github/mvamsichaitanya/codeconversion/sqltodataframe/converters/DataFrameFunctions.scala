//todo: implement OR condition in case when
package io.github.mvamsichaitanya.codeconversion.sqltodataframe.converters

import io.github.mvamsichaitanya.codeconversion.sqltodataframe.SqlToDfConversion._
import io.github.mvamsichaitanya.codeconversion.sqltodataframe.utils.CommonUtils._
import io.github.mvamsichaitanya.codeconversion.sqltodataframe.utils.ParsingUtils.getFilters
import io.github.mvamsichaitanya.codeconversion.sqltodataframe.{convertToFilter, convertToFunction, isFunction}

/**
  * DataFrame Function Converters
  */

object DataFrameFunctions {

  /**
    * Function to convert Function arguments
    */
  val processArgument: String => String = (col: String) => {

    if (isVariable(col))
      convertToVariable(col)
    else if (isFunction(col))
      convertToFunction(col)
    else
      """$"""" + col +"""""""
  }

  /**
    *
    * @param col : Column
    * @return : Convert case when statements to  when().otherwise of data frame
    */
  def processCaseWhen(col: String): String = {

    def getFilterAndVariable(condition: String): (String, String) = {
      val filterAndVar = getArguments(condition.replace("THEN", ";"), ';')
      var filters = getFilters(filterAndVar.head.replace(" AND ", ";"))
      filters = filters.map(f => convertToFilter(f))
      val filter = filters.mkString(" && ")
      val variable = processArgument(filterAndVar.last.replace(" END",""))
      (filter, variable)
    }

    var result = ""
    val whenStmt =
      if (col.trim.charAt(0) == '(')
        getBetweenBraces(col)._1
      else
        col.trim
    val conditions =
      getArguments(whenStmt.replace("WHEN ", ";"), ';').tail.
        map(cond => cond.replace(";", "WHEN "))

    val length = conditions.length - 1
    var closingBraces = ""
    for (index <- conditions.indices) {

      index match {
        case `length` =>
          if (conditions(index).toUpperCase.contains("ELSE")) {

            val lastCond = getArguments(conditions(index).replace("ELSE", ";"), ';')
            val whenCond = getFilterAndVariable(lastCond.head)
            val elseVar = processArgument(lastCond.last.replace(" END", ""))

            result +=
              s"""when(${whenCond._1},${whenCond._2}).
                 |    otherwise($elseVar)""".stripMargin
          }
          else {
            val filterAndVar = getFilterAndVariable(conditions(index))
            val filter = filterAndVar._1
            val variable = filterAndVar._2

            result += s"""when($filter,${variable.replace(" END", "")})""".stripMargin
          }

        case _ =>
          val filterAndVar = getFilterAndVariable(conditions(index))
          val filter = filterAndVar._1
          val variable = filterAndVar._2

          result +=
            s"""when($filter,$variable).
               |    otherwise(""".stripMargin
          closingBraces += ")"
      }
    }

    result + closingBraces
  }

  /**
    *
    * @param col : Column
    * @return converts to data frame specific count or countDistinct function
    */
  def processCount(col: String): String = {
    val arr =
      if (col.contains("COUNT"))
        col.split("COUNT")
      else
        col.split("count")
    val tuple = getBetweenBraces(arr.tail.mkString("count"))
    val func =
      if (tuple._1.contains("DISTINCT"))
        "countDistinct"
      else
        "count"
    val result =
      tuple._1.
        replace("DISTINCT", "").trim
    val index = tuple._2
    val procResult = processArgument(result)

    arr(0) + func + "(" + procResult + ")" +
      arr.tail.mkString("count").substring(index)
  }

  /**
    *
    * @param funcStr : Lower case of function
    * @param col : Column
    * @return converts to data frame specific function
    */
  def processSingleArgumentFunctions(funcStr: String, col: String): String = {

    val arr =
      if (col.contains(funcStr.toUpperCase))
        col.split(funcStr.toUpperCase)
      else
        col.split(funcStr)
    val tuple = getBetweenBraces(arr.tail.mkString(funcStr))
    val result = tuple._1
    val index = tuple._2

    val procResult = processArgument(result)

    arr(0) + funcStr + "(" + procResult + ")" +
      arr.tail.mkString(funcStr).substring(index)

  }

  def processTrim(col: String): String = processSingleArgumentFunctions("trim", col)

  def processSum(col: String): String = processSingleArgumentFunctions("sum", col)

  def processMax(col: String): String = processSingleArgumentFunctions("max", col)

  def processMin(col: String): String = processSingleArgumentFunctions("min", col)

  def processSqrt(col: String): String = processSingleArgumentFunctions("sqrt", col)

  /**
    *
    * @param funcStr : Lower case of function
    * @param col : Column
    * @param separator : Seperator of arguments
    * @return converts to data frame specific function
    */
  def processMultiArgumentFunctions(funcStr: String,
                                    col: String,
                                    separator: Char = ','): String = {
    val arr =
      if (col.contains(funcStr.toUpperCase))
        col.split(funcStr.toUpperCase)
      else
        col.split(funcStr)
    val tuple = getBetweenBraces(arr.tail.mkString(funcStr))
    val result = tuple._1
    val index = tuple._2
    val arguments = getArguments(result.trim, separator)

    for (i <- arguments.indices) {
      arguments(i) = processArgument(arguments(i))
    }

    arr(0) + s"$funcStr(" + arguments.mkString(",") + ")" +
      arr.tail.mkString(funcStr).substring(index)
  }

  def processCoalesce(col: String): String = processMultiArgumentFunctions("coalesce", col)

  def processConcat(col: String): String = processMultiArgumentFunctions("concat", col)

  def processTranslate(col: String): String = processMultiArgumentFunctions("translate", col)

  def processSubStr(col: String): String = processMultiArgumentFunctions("substr", col)

  def processInStr(col: String): String = processMultiArgumentFunctions("instr", col)

  /**
    *
    * @param column : Column
    * @return Converts Arithmetic equation to data frame specific
    */
  def processArithmeticEquation(column: String): String = {

    if (isNumeric(column))
      return column
    if (column.contains('*') && column.trim.split('*').length <= 1)
      return """$"""" + column +"""""""
    val tuple = getBetweenBraces(column.trim)

    val col =
      if (tuple._2 == column.length || tuple._2 == column.length - 1)
        tuple._1
      else
        column

    val operator = getOutermostOperator(col)
    val arguments = getArguments(col, operator).map(processArgument)

    if (arguments.length == 2 && isDateColumn(arguments(0)) && Array('+', '-').contains(operator)) {
      operator match {
        case '+' =>
          if (!isNumeric(arguments(1)))
            throw new Exception("adding date column with non numeric column")
          s"date_add(${arguments(0)},${arguments(1)})"
        case '-' =>

          if (isNumeric(arguments(1)))
            s"date_sub(${arguments(0)},${arguments(1)})"
          else
            s"datediff(${arguments(0)},${arguments(1)})"
      }
    }
    else {
      "(" + arguments.mkString(s" $operator ") + ")"
    }

  }

  /**
    *
    * @param col volumn
    * @return converts to data frame specific function
    */
  def processCast(col: String): String = {
    val arr =
      if (col.contains("CAST"))
        col.split("CAST")
      else
        col.split("cast")

    val tuple = getBetweenBraces(arr.tail.mkString("cast"))
    val result = tuple._1
    val index = tuple._2
    val castElements =
      getArguments(result.trim.replace(" AS ", ";"), ';')
    if (castElements.length != 2)
      throw new Exception(s"${castElements.length} " +
        s"arguments found for cast function")
    castElements(0) = processArgument(castElements(0))

    s"""${castElements(0)}.cast("${castElements(1)}")""" +
      arr.tail.mkString("cast(").substring(index)
  }

}
