package io.github.mvamsichaitanya.codeconversion.sqltodataframe.utils

import net.sf.jsqlparser.parser.{CCJSqlParserManager, CCJSqlParserUtil}
import net.sf.jsqlparser.statement.select._
import net.sf.jsqlparser.util.TablesNamesFinder
import net.sf.jsqlparser.statement.insert.Insert
import org.apache.commons.lang3.StringUtils
import CommonUtils._
import io.github.mvamsichaitanya.codeconversion.sqltodataframe.SqlToDfConversion.variables
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions._
import java.io.StringReader

/**
  * Parsing sql  using JSqlParser  helps to Convert sql statements to Data frames
  **/
object ParsingUtils {

  /**
    * Throws exception if sql is not a valid one
    *
    * @param sql : Sql string
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
    * Returns List of join conditions
    **/
  def getJoins(sql: String): List[Join] = {

    val statement = CCJSqlParserUtil.parse(sql)
    val select = statement.asInstanceOf[Select]
    val joins = select.getSelectBody.
      asInstanceOf[PlainSelect].getJoins

    joins match {
      case null => List[Join]()
      case _ => joins.toList
    }
  }

  /**
    * Returns from table
    **/
  def getFrom(sql: String): FromItem = {
    val statement = CCJSqlParserUtil.parse(sql)
    val select = statement.asInstanceOf[Select]
    val from = select.getSelectBody.
      asInstanceOf[PlainSelect].getFromItem
    from
  }

  /**
    * Returns List of tables or views used
    **/
  def getTables(sql: String): List[String] = {

    val pm = new CCJSqlParserManager()
    val statement = pm.parse(new StringReader(sql))
    val tablesNamesFinder = new TablesNamesFinder()
    tablesNamesFinder.getTableList(statement).
      toList.
      filterNot(isNested)

  }

  /**
    * Returns List[conditions]
    **/
  def getFilterConditions(sql: String): List[String] = {

    val statement = CCJSqlParserUtil.parse(sql)
    val select = statement.asInstanceOf[Select]
    val where = select.getSelectBody.asInstanceOf[PlainSelect].getWhere
    val whereStmt = if (where == null) "" else where
    val filters = getFilters(whereStmt.toString.replace(" AND ", ";"))
    filters
  }

  /**
    * Returns List of  groupBy columns
    **/
  def getGroupByColumns(sql: String): List[String] = {

    val statement = CCJSqlParserUtil.parse(sql)
    val select = statement.asInstanceOf[Select]
    val groupBy = select.getSelectBody.asInstanceOf[PlainSelect].
      getGroupByColumnReferences
    val result =
      if (groupBy != null)
        groupBy.mkString(";").split(";").toList
      else
        List[String]()
    val selectColumns = getSelectColumns(sql)
    val alias = getSelectColumnsAlias(sql).
      map(kv => (kv._2, kv._1))
    val groupBycols = result.map(x =>
      if (StringUtils.isNumeric(x))
        selectColumns(x.toInt - 1)
      else
        alias.getOrElse(x, x))
    if (groupBycols == getSelectColumns(sql))
      List[String]()
    else
      groupBycols
  }

  /**
    * Returns List of  columns in select statements
    **/
  def getSelectColumns(sql: String): List[String] = {
    val statement = CCJSqlParserUtil.parse(sql)
    val select = statement.asInstanceOf[Select]
    val selectItems = select.getSelectBody.
      asInstanceOf[PlainSelect].getSelectItems
      .mkString(";").split(";").toList.
      map(x =>
        if (x.contains(" AS "))
          x.split(" AS ").init.mkString(" AS ")
        else if (x.contains(" as "))
          x.split(" as ").init.mkString(" as ")
        else
          x)
    selectItems
  }

  /**
    * Returns List of orderBy columns
    **/
  def getOrderBy(sql: String): List[(OrderByElement, Boolean)] = {
    val statement = CCJSqlParserUtil.parse(sql)
    val select = statement.asInstanceOf[Select]
    val orederBy = select.getSelectBody.
      asInstanceOf[PlainSelect].getOrderByElements

    orederBy.toList.map(x => (x, x.isAsc))
  }

  /**
    * Returns true if sql statement contains Distinct
    **/
  def isDistinct(sql: String): Boolean = {
    val statement = CCJSqlParserUtil.parse(sql)
    val select = statement.asInstanceOf[Select]
    val groupBy = select.getSelectBody.asInstanceOf[PlainSelect].
      getGroupByColumnReferences
    val result =
      if (groupBy != null)
        groupBy.mkString(";").split(";").toList
      else
        List[String]()
    val alias = getSelectColumnsAlias(sql).map(kv => (kv._2, kv._1))
    val selectCols = getSelectColumns(sql)
    val groupBycols = result.map(x =>
      if (StringUtils.isNumeric(x))
        selectCols(x.toInt - 1)
      else
        alias.getOrElse(x, x))
    val distinct = select.getSelectBody.asInstanceOf[PlainSelect].getDistinct
    if (distinct != null || groupBycols == selectCols)
      true
    else
      false
  }

  /**
    * Returns Map[TableName,AliasName]
    **/
  def getTableAlias(sql: String): Map[String, String] = {
    var result = scala.collection.mutable.Map[String, String]()
    val statement = CCJSqlParserUtil.parse(sql)
    val select = statement.asInstanceOf[Select]
    val from = select.getSelectBody.asInstanceOf[PlainSelect].
      getFromItem.toString.split(" ")
    val alias = if (from.length == 1) from(0) else from(1)
    result = result + (getTable(from(0)) -> getTable(alias))
    val joins = select.getSelectBody.asInstanceOf[PlainSelect].getJoins
    if (joins != null)
      joins.foreach { join => {
        val tableName = join.getRightItem.
          toString.split(" ")
        val as =
          if (tableName.length == 1)
            tableName(0)
          else
            tableName(1)

        result += (getTable(tableName(0)) -> getTable(as))
      }
      }
    result.toMap
  }

  /**
    * Returns Map[column,aliasColumn]
    **/
  def getSelectColumnsAlias(sql: String): Map[String, String] = {
    val statement = CCJSqlParserUtil.parse(sql)
    val select = statement.asInstanceOf[Select]
    val selectColumns = select.getSelectBody.
      asInstanceOf[PlainSelect].getSelectItems
      .mkString(";").split(";").toList

    select.getSelectBody.
      asInstanceOf[PlainSelect]
    selectColumns.filter(col => col.toUpperCase.contains(" AS ")).
      map(col =>
        if (col.contains(" AS "))
          (col.split(" AS ").init.mkString(" AS "), col.split(" AS ").last)
        else
          (col.split(" as ")(0), col.split(" as ")(1))).toMap
  }

  /**
    * Returns join type
    **/
  def getJoinType(join: Join): String = {
    join match {
      case _ if join.isCross => "cross"
      case _ if join.isLeft => "left"
      case _ if join.isOuter => "left_outer"
      case _ => "inner"
    }
  }

  /**
    * Gets Nested sql statement and return its alias name
    * Map[NestedSql,AliasName]
    **/
  def getNested(sql: String): Map[String, String] = {
    val statement = CCJSqlParserUtil.parse(sql)
    val select = statement.asInstanceOf[Select]
    val from = select.getSelectBody.asInstanceOf[PlainSelect].getFromItem
    val joins = getJoins(sql)
    val allTables = from :: joins.map(join => join.getRightItem)
    allTables.filter(isNested).
      map(nestedItem =>
        (nestedItem.toString.split(" ").init.mkString(" "),
          nestedItem.getAlias.getName)).
      toMap
  }

  /**
    *
    * @param whereCondition where condition
    * @return List of filters in where condition
    */
  def getFilters(whereCondition: String): List[String] = {
    var result = List[String]()

    def go(i: Int): Unit = {
      val stack = scala.collection.mutable.Stack[Char]()
      var flag = 0
      var index = i
      val argument = for (c <- whereCondition.substring(i); if flag == 0) yield {
        index += 1
        if ((c == ';' || c == ')') && stack.isEmpty) {
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
      if (index != whereCondition.length)
        go(index)
    }

    go(0)
    val filters = result.toArray.reverse
    val l = filters.length

    def iterate(index: Int,
                result: List[String]): List[String] = {
      if (index < l) {
        if (filters(index).toUpperCase.contains(" BETWEEN "))
          iterate(index + 2,
            filters(index) + " AND " + filters(index + 1) :: result)
        else
          iterate(index + 1,
            filters(index) :: result)
      }
      else
        result
    }

    iterate(0, List[String]())
  }

  /**
    *
    * @param sqlStmt : SQL
    * @return table name of the insert into stmt
    */
  def getInsertIntoTable(sqlStmt: String): String = {


    val sql = extractVariables(sqlStmt)
    println(sql)

    val pm = new CCJSqlParserManager()
    val statement = pm.parse(new StringReader(sql))
    val insertInToStmt = statement.asInstanceOf[Insert]
    insertInToStmt.getTable.toString

  }

  /**
    *
    * @param sqlStmt SQL
    * @return Select statement inside insert into stmt
    */
  def getInsertIntoStmt(sqlStmt: String): String = {

    val sql = extractVariables(sqlStmt)

    val pm = new CCJSqlParserManager()
    val statement = pm.parse(new StringReader(sql))
    val insertInToStmt = statement.asInstanceOf[Insert]
    insertInToStmt.getSelect.toString
  }

  /**
    *
    * @param sql SQL
    * @return removes string interpolation
    */
  def extractVariables(sql: String): String = {
    var stmt = sql
    sql.split('{').tail.foreach(x => {
      if (!variables.contains(x.split('}')(0)))
        variables = x.split('}')(0) :: variables

      stmt = stmt.replace("'${" + x.split('}')(0) + "}'", x.split('}')(0))
      stmt = stmt.replace("${" + x.split('}')(0) + "}", x.split('}')(0))
    })
    stmt
  }

}