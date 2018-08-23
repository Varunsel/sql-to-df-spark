package io.github.mvamsichaitanya.codeconversion

trait CodeConversion {

  /**
    * Representing different types of
    * SqlStmt in Case Objects
    */
  sealed trait SqlStmtType

  case object AnonymousStmt extends SqlStmtType

  case object CreateStmt extends SqlStmtType

  case object InsertIntoStmt extends SqlStmtType

  /**
    * start method
    * contains code that converts
    */
  def start():Unit

}
