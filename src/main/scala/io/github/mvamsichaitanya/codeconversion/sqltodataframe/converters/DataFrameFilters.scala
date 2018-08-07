package io.github.mvamsichaitanya.codeconversion.sqltodataframe.converters

import io.github.mvamsichaitanya.codeconversion.sqltodataframe.SqlToDfConversion._
import io.github.mvamsichaitanya.codeconversion.sqltodataframe._
import io.github.mvamsichaitanya.codeconversion.sqltodataframe.utils.CommonUtils._

/**
  * DataFrame Filter Converters
  */

object DataFrameFilters {

  /**
    *
    * @param condition Condition of between filter
    * @return Converted data frame specific filter
    */
  def processBetweenFilter(condition: String): String = {

    val array = if (condition.contains(" BETWEEN "))
      condition.split(" BETWEEN ")
    else
      condition.split(" between ")
    val preBetween = array(0)
    val postBetween = array(1)
    val tuple = getBetweenBraces(postBetween)
    val rest =
      postBetween.substring(tuple._2).
        split("AND ").tail.mkString("AND ")
    val firstElement =
      if (isFunction(tuple._1))
        convertToFunction(tuple._1)
      else if (isVariable(tuple._1))
        convertToVariable(tuple._1)
      else
        """$"""" + tuple._1 +"""""""

    val secondElement =
      if (isFunction(rest))
        convertToFunction(rest)
      else if (isVariable(rest))
        convertToVariable(rest)
      else
        """$"""" + rest + """""""

    """$"""" + s"""${preBetween.trim}".between($firstElement,$secondElement)"""
  }

  /**
    *
    * @param condition : In filter conditon
    * @return converts to isin filter of data frame
    */
  def processInFilter(condition: String): String = {

    val array =
      if (condition.contains(" IN "))
        condition.split(" IN ")
      else
        condition.split(" in ")
    val preIn = array(0)
    val postIn = array.tail.mkString(" IN ")
    if (isSql(postIn))
      throw new Exception("Nested Sql 'IN' filter not yet implemented")
    else {
      val in = """$"""" + s"""$preIn".isin(Array${postIn.trim.replace(''', '"')}:_*)"""
      in
    }
  }


  /**
    * @param condition : condition containing comparision filter
    * @return data frame specific comparision filter
    **/
  def processComparisionFilters(condition: String): String = {

    def extractOperator(): String = {
      val result = ""
      val operators = comparisonFilters.keys
      operators.foreach(operator => {
        if (condition.contains(operator))
          return operator
      })
      result
    }

    val operator = extractOperator()
    val replaceOp = comparisonFilters(operator)
    val arr = condition.split(operator)

    val preOperator =
      if (isFunction(arr(0)))
        convertToFunction(arr(0))
      else if (isVariable(arr(0)))
        convertToVariable(arr(0))
      else
        """$"""" + arr(0).trim +"""""""
    val postOperator =
      if (isFunction(arr(1)))
        convertToFunction(arr(1))
      else if (isVariable(arr(1)))
        convertToVariable(arr(1))
      else
        """$"""" + arr(1).trim +"""""""

    s"""$preOperator $replaceOp $postOperator"""
  }

  /**
    * @param condition : condition containing isNotNull filter
    * @return data frame specific isNotNull filter
    **/
  def processIsNotNullFilter(condition: String): String = {
    val cond = condition.trim.toUpperCase.replace(" IS NOT NULL", "")

    val convertedVar =
      if (isFunction(cond))
        convertToFunction(cond)
      else if (isVariable(cond))
        convertToVariable(cond)
      else
        """$"""" + cond.trim +"""""""

    s"""$convertedVar.isNotNull"""

  }
}
