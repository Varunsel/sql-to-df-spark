package io.github.mvamsichaitanya.codeconversion.sastosparksql

import io.github.mvamsichaitanya.codeconversion.TestCodeBase


class ConcatAndCastFuncTest extends TestCodeBase {

  /*

SAMPLE TABLE:-
    student_id,name,age,branch,college
   */


  var sparkSql: SparkSql = _

  "|| of sas" should {

    "convert to concat of spark sql" in {

      val inputSasSqlStmt =
        """SELECT
                          	DISTINCT student_id AS id,
                               branch||college AS branch_college
                          FROM
                          	STUDENT"""


      val expectedOutputSql =
        """SELECT	DISTINCT student_id AS id,
                           CONCAT(branch,college ) AS branch_college
                            FROM
                            STUDENT"""
      sparkSql = new SparkSql(inputSasSqlStmt, "")
      sparkSql.create()
      val convertedSql = sparkSql.convertedSql
      assert(compareSql(convertedSql, expectedOutputSql))


    }

    "multiline || to concat of spark sql" in {

      val inputSasSqlStmt =
        """SELECT
                          	DISTINCT student_id AS id,
                               weight||'_'||(CASE
                            WHEN WEIGHT BETWEEN 0 AND 50 THEN 'LOW'
                            WHEN WEIGHT BETWEEN 51 AND 70 THEN 'MEDIUM'
                            WHEN WEIGHT BETWEEN 71 AND 100 THEN 'HIGH'
                            ELSE 'VERY HIGH'
                            END) AS NEWWEIGHT
                          FROM
                          	STUDENT"""


      val expectedOutputSql =
        """SELECT
                        DISTINCT student_id AS id,
                        CONCAT( weight,'_',(CASE
                        WHEN WEIGHT BETWEEN 0 AND 50 THEN 'LOW'
                        WHEN WEIGHT BETWEEN 51 AND 70 THEN 'MEDIUM'
                        WHEN WEIGHT BETWEEN 71 AND 100 THEN 'HIGH'
                        ELSE 'VERY HIGH'
                        END) )  AS NEWWEIGHT
                        FROM
                        STUDENT"""

      sparkSql = new SparkSql(inputSasSqlStmt, "")
      sparkSql.create()
      val convertedSql = sparkSql.convertedSql
      assert(compareSql(convertedSql, expectedOutputSql))


    }

    "|| with translate function" in {

      val inputSasSqlStmt =
        """SELECT
                              	DISTINCT student_id AS id,
                              	trim( branch||'_'||translate(trim(college),' &/!@#$%^*;?<>":=+-~`\|[]{}_,.()''','')) AS branch_college
                              FROM
                              	student
                              """


      val expectedOutputSql =
        """SELECT DISTINCT
                           student_id AS id,
                           trim(
                                 CONCAT( branch,'_',translate(trim(college),Regex,'') ) ) AS branch_college
                            FROM
                                student
                                """

      sparkSql = new SparkSql(inputSasSqlStmt, "")
      sparkSql.create()
      val convertedSql = sparkSql.convertedSql
      assert(compareSql(convertedSql, expectedOutputSql))


    }
    "multi || functions" in {
      val inputSasSqlStmt =
                         """SELECT
                              	DISTINCT student_id AS id,
                              	trim( branch||'_'||translate(trim(college),' &/!@#$%^*;?<>":=+-~`\|[]{}_,.()''','')) AS branch_college
                              FROM
                              	student
                               where
                               'CSE'||'_'||college != branch_college
                              """


      val expectedOutputSql =
        """SELECT DISTINCT
                           student_id AS id,
                           trim(
                                 CONCAT( branch,'_',translate(trim(college),Regex,'') ) ) AS branch_college
                            FROM
                                student
                           where
                               CONCAT('CSE','_',college) != branch_college
                                """

      sparkSql = new SparkSql(inputSasSqlStmt, "")
      sparkSql.create()
      val convertedSql = sparkSql.convertedSql
      assert(compareSql(convertedSql, expectedOutputSql))

    }

  }

  ":: of sas" should {

    "convert to cast function of spark sql" in {

      val inputSasSqlStmt =
        """SELECT
                          	DISTINCT student_id :: int AS id
                          FROM
                          	STUDENT"""


      val expectedOutputSql =
        """SELECT	DISTINCT cast(student_id as int) AS id
                            FROM
                            STUDENT"""
      sparkSql = new SparkSql(inputSasSqlStmt, "")
      sparkSql.create()
      val convertedSql = sparkSql.convertedSql
      assert(compareSql(convertedSql, expectedOutputSql))


    }

    "multiline :: to cast of spark sql" in {

      val inputSasSqlStmt =
        """SELECT
                          	DISTINCT student_id AS id,
                         (CASE
                            WHEN WEIGHT BETWEEN 0 AND 50 THEN 'LOW'
                            WHEN WEIGHT BETWEEN 51 AND 70 THEN 'MEDIUM'
                            WHEN WEIGHT BETWEEN 71 AND 100 THEN 'HIGH'
                            ELSE 'VERY HIGH'
                            END) :: char(64) AS NEWWEIGHT
                          FROM
                          	STUDENT"""


      val expectedOutputSql =
        """SELECT
                        DISTINCT student_id AS id,
                        cast((CASE
                        WHEN WEIGHT BETWEEN 0 AND 50 THEN 'LOW'
                        WHEN WEIGHT BETWEEN 51 AND 70 THEN 'MEDIUM'
                        WHEN WEIGHT BETWEEN 71 AND 100 THEN 'HIGH'
                        ELSE 'VERY HIGH'
                        END) as char(64))  AS NEWWEIGHT
                        FROM
                        STUDENT"""

      sparkSql = new SparkSql(inputSasSqlStmt, "")
      sparkSql.create()
      val convertedSql = sparkSql.convertedSql
      assert(compareSql(convertedSql, expectedOutputSql))


    }

    ":: with translate function and ||" in {

      val inputSasSqlStmt =
        """SELECT
                              	DISTINCT student_id AS id,
                              	trim( branch||'_'||translate(trim(college),' &/!@#$%^*;?<>''','')) :: char(64) AS branch_college
                              FROM
                              	student
                              """


      val expectedOutputSql =
        """SELECT DISTINCT
                           student_id AS id,
                           cast(trim(
                                 CONCAT( branch,'_',translate(trim(college),Regex,'') ) ) as char(64)) AS branch_college
                            FROM
                                student
                                """

      sparkSql = new SparkSql(inputSasSqlStmt, "")
      sparkSql.create()
      val convertedSql = sparkSql.convertedSql
      assert(compareSql(convertedSql, expectedOutputSql))


    }
  }

  "Both :: and || of sas in same element" should {
    "should convert correctly " in {
      val inputSasSqlStmt =
        """SELECT
                          	DISTINCT student_id AS id,
                               (branch||'_'||%str(%')&postfix%str(%')) :: char(64) AS branch_college
                          FROM
                          	STUDENT"""


      val expectedOutputSql =
        """select distinct student_id as id,
           CAST(
           (
          CONCAT(branch,'_','${postfix}')
          )
          as char(64) )  as branch_college
          FROM
          	STUDENT"""

      sparkSql = new SparkSql(inputSasSqlStmt, "")
      sparkSql.create()
      val convertedSql = sparkSql.convertedSql
      assert(compareSql(convertedSql, expectedOutputSql))

    }
  }
}
