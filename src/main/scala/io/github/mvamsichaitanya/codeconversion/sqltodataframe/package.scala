package io.github.mvamsichaitanya.codeconversion

/**
  * Intermediate code for sql to DataFrame conversion
  */

package object sqltodataframe {

  import io.github.mvamsichaitanya.codeconversion.sqltodataframe.converters.DataFrameFunctions._
  import io.github.mvamsichaitanya.codeconversion.sqltodataframe.converters.DataFrameFilters._
  import io.github.mvamsichaitanya.codeconversion.sqltodataframe.constants.StringConstants.EmptyString
  import io.github.mvamsichaitanya.codeconversion.sqltodataframe.utils.CommonUtils._
  import collection.immutable.ListMap


  /**
    * [[ListMap]] is used because  order of functions processed is important for comparision filters
    * [[comparisonFilters]] : Comparision filters and corresponding mapping to DataFrame filters
    * [[filterIdentifiers]] : List of strings used to identify filter
    * [[filters]] : Map of filter identifiers to conversion functions
    */

  val comparisonFilters: ListMap[String, String] =
    ListMap(">=" -> ">=",
      "<=" -> "<=",
      "<" -> "<",
      ">" -> ">",
      "!=" -> "=!=",
      "=" -> "===")

  val filterIdentifiers: List[String] = List("between", " in ", "is not null")

  val filters: ListMap[String, String => String] = ListMap(
    "between" -> processBetweenFilter,
    "comparision" -> processComparisionFilters,
    " in " -> processInFilter,
    "is not null" -> processIsNotNullFilter
  )

  /**
    * [[arithmeticOperators]] : List of arithmetic operators
    * [[aggFunctionIdentifiers]] : List of strings used to identify agg functions
    * [[aggFunctions]] : Map of agg Function identifiers to conversion functions
    * [[functionsIdentifiers]] : List of strings used to identify functions
    * [[functions]] : Map of Function identifiers to conversion functions
    */

  val arithmeticOperators=List('*','/','+','-')

  val aggFunctions: ListMap[String, String => String] = ListMap(
    "count(" -> processCount,
    "sum(" -> processSum,
    "max(" -> processMax,
    "min(" -> processMin
  )

  val aggFunctionIdentifiers: List[String] = aggFunctions.keys.toList

  val functions: ListMap[String, String => String] =
    ListMap[String, String => String](
      "coalesce(" -> processCoalesce,
      "trim(" -> processTrim,
      "concat(" -> processConcat,
      "translate(" -> processTranslate,
      "case when " -> processCaseWhen,
      "substr(" -> processSubStr,
      "instr(" -> processInStr,
      "cast(" -> processCast,
      "sqrt(" -> processSqrt
           , "+" -> processArithmeticEquation,
            "-" -> processArithmeticEquation,
            "*" -> processArithmeticEquation,
            "/" -> processArithmeticEquation
    ) ++ aggFunctions

  val functionsIdentifiers: List[String] = functions.keys.toList ::: aggFunctionIdentifiers ::: arithmeticOperators.map(_.toString)

  /**
    * [[isFunction]] => Take String as input and returns true if it contains function identifier
    * [[isAggFunction]] => Take String as input and returns true if it contains agg function identifier
    */
  def isFunction(s: String): Boolean = {
    functionsIdentifiers.foreach(identifier => {
      if (s.toLowerCase.contains(identifier))
        return true
    })
    false
  }

  def isAggFunction(s: String): Boolean = {
    aggFunctionIdentifiers.foreach(identifier => {
      if (s.toLowerCase.contains(identifier))
        return true
    })
    false
  }

  /**
    * Below Functions are used to convert functions in columns to DataFrame Specific
    *
    * [[convertToFunction]] => takes string as input which may contain nested sql functions and
    * converts them to DataFrame specific functions
    * [[convertToAggFunction]] => takes string as input which may contain nested sql functions and
    * converts them to DataFrame specific agg functions
    * [[getOutermostIdentifier]] => returns outermost function identifier of nested function
    */
  def convertToFunction(col: String): String = {

    if(isArithmeticOuterFunction(col))
      processArithmeticEquation(col)
    else {
      val identifier = getOutermostIdentifier(col.toLowerCase)
      functions(identifier).apply(col)
    }
  }

  /**
    *
    * @param col
    * @return
    */
  def convertToAggFunction(col: String): String = {

    if(isArithmeticOuterFunction(col))
      processArithmeticEquation(col)
    else {
      val identifier = getOutermostIdentifier(col.toLowerCase)
      functions(identifier).apply(col)
    }
  }

  def isArithmeticOuterFunction(column: String): Boolean = {

    if(containArithmeticOperators(column)) {
      val tuple = getBetweenBraces(column.trim)
      val col =
        if (tuple._2 == column.length)
          tuple._1
        else
          column
      arithmeticOperators.foreach(operator => {
        val args = getArguments(col, operator)
        if (args.length > 1)
          return true
      })
    }
    false
  }

  def getOutermostIdentifier(col: String): String = {
    var result = EmptyString
    var index = col.length

      functionsIdentifiers.foreach(identifier => {
        if (col.indexOf(identifier) != -1 && col.indexOf(identifier) < index) {
          result = identifier
          index = col.indexOf(identifier)
        }
      })

    result
  }

  /**
    * Below function converts Where condition to DataFrame specific filters
    *
    * @param condition : Condition statement
    * @return : Filter of DataFrame Type
    */
  def convertToFilter(condition: String): String = {
    val result = condition
    val operators = comparisonFilters.keys.toList
    filterIdentifiers.foreach(identifier => {
      if (condition.toLowerCase.contains(identifier))
        return filters(identifier).apply(condition)
    })
    operators.foreach(operator => {
      if (condition.contains(operator))
        return filters.apply("comparision").apply(condition)
    })
    result
  }



}
