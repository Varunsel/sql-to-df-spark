package io.github.mvamsichaitanya.codeconversion.sastosparksql

import io.github.mvamsichaitanya.codeconversion.TestCodeBase

class OtherFunctionsTest extends TestCodeBase {

  /*

SAMPLE TABLE:-
   student_id,name,age,branch,college
  */

  var sparkSql: SparkSql = _

  "& function" should {

    "convert to string interpolation without %STR macro function " in {

      val inputSasSqlStmt =
        """SELECT
                          	DISTINCT student_id AS id,
                               branch||college AS &branch_college
                          FROM
                          	STUDENT"""


      val expectedOutputSql =
        """SELECT	DISTINCT student_id AS id,
                           CONCAT(branch,college ) AS ${branch_college}
                            FROM
                            STUDENT"""
      sparkSql = new SparkSql(inputSasSqlStmt, "")
      sparkSql.create()
      val convertedSql = sparkSql.convertedSql
      assert(compareSql(convertedSql, expectedOutputSql))

    }

    "convert to string interpolation with %STR macro function " in {

      val inputSasSqlStmt =
        """SELECT
                          	DISTINCT student_id AS id,
                               branch||'_'||%str(%')&postfix%str(%') AS &branch_college
                          FROM
                          	STUDENT"""


      val expectedOutputSql =
        """SELECT	DISTINCT student_id AS id,
                           CONCAT(branch,'_','${postfix}') AS ${branch_college}
                            FROM
                            STUDENT"""
      sparkSql = new SparkSql(inputSasSqlStmt, "")
      sparkSql.create()
      val convertedSql = sparkSql.convertedSql
      assert(compareSql(convertedSql, expectedOutputSql))
    }

  }

}
