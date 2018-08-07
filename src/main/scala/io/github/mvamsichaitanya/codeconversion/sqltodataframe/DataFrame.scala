package io.github.mvamsichaitanya.codeconversion.sqltodataframe

import net.sf.jsqlparser.statement.select.Join
import io.github.mvamsichaitanya.codeconversion.sqltodataframe.utils.ParsingUtils._
import io.github.mvamsichaitanya.codeconversion.sqltodataframe.utils.CommonUtils._
import SqlToDfConversion._
import io.github.mvamsichaitanya.codeconversion.sqltodataframe.constants.StringConstants._

/**
  * Custom DataFrame class
  *
  * @param sqlStmt     SQL
  * @param dfName      table name
  * @param sqlStmtType Type of sql
  */
class DataFrame(sqlStmt: String,
                dfName: String,
                sqlStmtType: SqlStmtType = CreateStmt) {

  /**
    * filters : All filters of DataFrame
    * joinFilters : Filter Specific to columns which are present only after join
    * selectFilters : Filter Specific to columns which are present in select statement
    */
  var filters: List[String] = List.empty
  var joinFilters: List[String] = List.empty
  var selectFilters: List[String] = List.empty
  /**
    * selectCols : Columns in select statement
    * groupByCols : Columns of group by
    * aggCols : Aggregated columns
    */
  var selectCols: List[String] = List.empty
  var groupByCols: List[String] = List.empty
  var aggCols: List[String] = List.empty
  /**
    * tableAlias : Map of table name and alias name
    * selectColsAlias : Map of Column name and alias name
    */
  var tableAlias: Map[String, String] = Map.empty
  var selectColsAlias: Map[String, String] = Map.empty
  /**
    * sql : Input sql statement
    * dataFrameCode : Resultant DataFrame code
    */
  var sql: String = EmptyString
  var dataFrameCode: String = EmptyString

  create()

  /**
    * Function Which creates DataFrame of Input SQL and writes to output file
    */
  private def create(): Unit = {

    if (sqlStmtType == CreateStmt)
      println(s"started creating DataFrame $dfName")
    else
      println(s"started appending sql $sqlStmt to $dfName")


    /**
      * Removes string interpolated variables and add them to [[variables]]
      */
    sql = extractVariables(sqlStmt)

    validateSql(sql)

    /*
    Splits select statement of sql to select cols and aggregated cols
    and initialize groupByCols
     */
    val cols = splitSelectColumns(sql)
    selectCols = cols._1
    aggCols = cols._2
    groupByCols = getGroupByColumns(sql)

    /*
     Initialize selectColsAlias,tableAlias
      */
    selectColsAlias = getSelectColumnsAlias(sql)
    tableAlias = getTableAlias(sql)
    /*
    List of tables used in sql
     */
    val tables = getTables(sql)
    /*
       Loading Base tables that are not loaded till now
      */
    tables.foreach(table => {
      if (table.contains('.')) {
        val tableName = table.split('.')(1)
        if (!tablesLoaded.contains(tableName)) {
          fw.write(s"""val $tableName  =  spark.read.table("$table") \n \n""")
          tablesLoaded = tableName :: tablesLoaded
        }
      }
    })
    /*
     gets JOINS present in sql statement
     */
    val joins = getJoins(sql)

    /*
      gets list of conditions present in sql statement
     */
    var conditions = getFilterConditions(sql)
    conditions = conditions.map { condition =>
      var cond = condition
      aggCols.foreach(col => {
        cond =
          cond.
            replace(col + " ", selectColsAlias.getOrElse(col, col) + " ")
      })
      cond
    }
    conditions = conditions.map { condition =>
      var cond = condition
      selectCols.foreach(col => {
        cond = cond.replace(col + " ", selectColsAlias.getOrElse(col, col) + " ")
      })
      cond
    }
    /*
    Initializing DataFrame with DataSet from which it is transformed
    */
    addFromTable()

    /*
    Adding joins to data frames if exists
     */

    addJoins(joins)

    /*
    Extracting filter conditions from sql Statement and
    converting them to DataFrame specific filters
    */

    convertFilters(conditions)
    splitFilters()

    /*
    Adding Filter conditions Specific to join columns
     */

    addJoinFilters()

    /*
    Adds groupBy statement and aggregations if Exists
     */

    addGroupByAndAggregations()

    /*
    Adding Select statement
     */

    addSelectStatement()

    /*
    Adding filters specific to select columns
     */

    addSelectFilters()

    /*
    adds distinct statement
     */
    if (isDistinct(sql))
      dataFrameCode += ".\n distinct"

    /*
    Writing DataFrame to file
     */
    if (fw != null)
      write()
  }

  /**
    *
    * @param sql Sql
    * @return removes string interpolation
    */
  private def extractVariables(sql: String): String = {
    var stmt = sql
    sql.split('{').tail.foreach(x => {
      if (!variables.contains(x.split('}')(0)))
        variables = x.split('}')(0) :: variables

      stmt = stmt.replace("'${" + x.split('}')(0) + "}'", x.split('}')(0))
      stmt = stmt.replace("${" + x.split('}')(0) + "}", x.split('}')(0))
    })
    stmt
  }

  /**
    * Function to add from table of sql
    * if from table is a nested then creates a new dataframe for the nested statement
    */
  private def addFromTable(): Unit = {

    val fromTable = getFrom(sql)

    isNested(fromTable) match {
      case true =>
        val nestedTblName = dfName + "_" + fromTable.getAlias.getName
        val nestedSql = fromTable.toString.split(" ").init.mkString(" ")
        new DataFrame(nestedSql, nestedTblName, CreateStmt)
        dataFrameCode +=
          nestedTblName + s""".as("${fromTable.getAlias.getName}")"""

      case false =>
        val table = getTable(fromTable.toString).split(" ")(0)
        if (tableAlias(table) != table)
          dataFrameCode = table + s""".as("${tableAlias(table)}")"""
        else
          dataFrameCode = table
    }

  }

  /**
    * Function to add select statement
    */
  private def addSelectStatement(): Unit = {

    val selectColumns = selectCols.
      map(col =>
        if (groupByCols.contains(col))
          selectColsAlias.getOrElse(col, col)
        else
          col)

    val select =
      selectColumns.
        map(col =>
          if (isFunction(col))
            (convertToFunction(col), selectColsAlias.getOrElse(col, ""))
          else if (isVariable(col))
            (convertToVariable(col), selectColsAlias.getOrElse(col, ""))
          else
            ("""$"""" + col +""""""", selectColsAlias.getOrElse(col, ""))).
        map(x =>
          if (x._2 != "")
            x._1 +s""".as("${x._2}")"""
          else
            x._1).mkString(",")

    val aggs =
      if (aggCols.isEmpty)
        EmptyString
      else
        "," +
          aggCols.
            map(col =>"""$"""" + selectColsAlias(col) +""""""").mkString(",")

    dataFrameCode += s""". \n select($select$aggs)"""

  }

  /**
    * converts sql joins to data frame joins
    *
    * @param joins : List of joins in sql
    */
  private def addJoins(joins: List[Join]): Unit = {
    joins.foreach(join => {

      isNested(join.getRightItem) match {

        case true =>

          val from = join.getRightItem
          val nestedTblName = dfName + "_" + from.getAlias.getName
          val nestedSql = from.toString.split(" ").init.mkString(" ")
          new DataFrame(nestedSql, nestedTblName, CreateStmt)
          val result =
            if (getJoinType(join) == "cross")
              s""".\n crossJoin($nestedTblName.as(${from.getAlias.getName}))"""
            else {
              val condition =
                join.getOnExpression.
                  toString.split(" = ").
                  map(x =>"""$"""" + x.trim +""""""")

              s""". \n join($nestedTblName.as(${from.getAlias.getName}),
                 |${condition(0)} === ${condition(1)},"${getJoinType(join)}")""".stripMargin
            }
          dataFrameCode += result

        case false =>

          val table = join.getRightItem.toString.split(" ")

          val result =
            if (getJoinType(join) == "cross")
              s""".\n crossJoin(${getTable(table(0))}.as("${tableAlias(getTable(table(0)))}"))"""
            else {
              val joinCondition = join.getOnExpression.toString
              var conditions = getFilters(joinCondition.replace(" AND ", ";"))
              conditions = conditions.
                map(condition => convertToFilter(condition)).
                map(_.trim)

              s""". \n join(${getTable(table(0))}.as("${tableAlias(getTable(table(0)))}"),
                 |${conditions.mkString("&&")},"${getJoinType(join)}")""".stripMargin
            }
          dataFrameCode += result

      }
    }
    )
  }

  /**
    * adds group by and aggregations
    */
  private def addGroupByAndAggregations(): Unit = {

    groupByCols match {
      case _ :: _ =>
        val groupByColumns = groupByCols.
          map(col =>
            if (isFunction(col))
              (convertToFunction(col), selectColsAlias.getOrElse(col, ""))
            //In some bad sql's variable is given as groupby
            else if (isVariable(col))
              (convertToVariable(col), selectColsAlias.getOrElse(col, ""))
            else
              ("""$"""" + col +""""""", selectColsAlias.getOrElse(col, ""))
          ).
          map(x =>
            if (x._2 != "")
              x._1 +s""".as("${x._2}")"""
            else
              x._1).mkString(",")
        dataFrameCode += s""".\ngroupBy($groupByColumns)"""
        addAggregations()
      case _ =>

    }
  }

  private def addAggregations(): Unit = {
    aggCols match {

      case _ :: _ =>
        dataFrameCode +=
          s""".\nagg(""" + aggCols.map(col => convertToAggFunction(col).trim +
            s""".as("${selectColsAlias(col)}")""").mkString(",") + ")"

      case _ =>
        throw new Exception(s"No Aggregate Columns found for $dfName")
    }
  }

  /*
   Adds filter conditions to DF code
   */
  private def addJoinFilters(): Unit = {

    joinFilters.foreach(filter => {
      dataFrameCode += s""".\nfilter($filter)"""
    })
  }

  private def addSelectFilters(): Unit = {

    selectFilters.foreach(filter => {
      dataFrameCode += s""".\nfilter($filter)"""
    })
  }

  /**
    * Convert all conditions to data frame specific
    *
    * @param conditions where conditions
    */
  private def convertFilters(conditions: List[String]): Unit = {
    filters = conditions.
      map(condition =>
        if (condition.toLowerCase.contains(" in "))
          processInFilter(condition)
        else
          convertToFilter(condition)).
      filter(_ != "")
  }

  /**
    * Splits select statement of sql to select cols and aggregated cols
    * and initialize groupByCols
    *
    * @param sql SQL
    * @return
    */
  private def splitSelectColumns(sql: String): (List[String], List[String]) = {

    val groupBycolumns = getGroupByColumns(sql)
    groupBycolumns match {
      case _ :: _ =>
        val allColumns = getSelectColumns(sql)
        val aggColumns = allColumns.filter(isAggFunction)
        val restColumns = allColumns.filterNot(isAggFunction)
        (restColumns, aggColumns)
      case _ =>
        (getSelectColumns(sql), List[String]())
    }
  }

  /*
  * Split filters into select statement filters and join filters
  */
  private def splitFilters(): Unit = {

    val cols = selectCols.
      map(col => selectColsAlias.getOrElse(col, col)) :::
      aggCols.map(col => selectColsAlias.getOrElse(col, col))

    selectFilters = filters.filter(cols.contains)
    joinFilters = filters.filterNot(cols.contains)
  }

  /*
    * IsIn Filter to be processed inside this code itself because there may need to add a join condition
    */
  private def processInFilter(condition: String): String = {

    val array =
      if (condition.contains(" IN "))
        condition.split(" IN ")
      else
        condition.split(" in ")
    val preIn = array(0)
    val postIn = array.tail.mkString(" IN ")
    if (isSql(postIn)) {
      val isInDf = dfName + "_IsIn"
      new DataFrame(postIn, isInDf, CreateStmt)
      val cols = getSelectColumns(postIn)
      if (cols.lengthCompare(1) > 0)
        throw new Exception(IsInException)
      dataFrameCode +=
        s""".
 join($isInDf,"$preIn" === "${cols.head}","left_semi")""".stripMargin
      ""
    }
    else {
      val in = """$"""" + s"""$preIn".isin(Array${postIn.trim.replace(''', '"')}:_*)"""
      in
    }

  }

  //writes Data frame code to file
  private def write(): Unit = {
    if (sqlStmtType == CreateStmt) {
      fw.write(s"""val $dfName = $dataFrameCode \n \n""")
      println(s"Successfully writen $dfName")

    }
    else {
      fw.write(
        s""" $dfName = $dfName.
           |union($dataFrameCode) \n \n""".stripMargin)

      println(s"Successfully appended to $dfName")
    }

  }

  override def toString: String =
    if (sqlStmtType == CreateStmt)
      s"""val $dfName = $dataFrameCode \n \n"""
    else
      s""" $dfName = $dfName.
         |union($dataFrameCode) \n \n""".stripMargin


}