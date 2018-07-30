package io.github.mvamsichaitanya.codeconversion.sqltodataframe

import io.github.mvamsichaitanya.codeconversion.TestCodeBase

class FiltersTest extends TestCodeBase {

  val DfName = "df"

  "between filter of sql" should {

    "convert to between filter of data frame" in {
      val inputSql =
        """select distinct student id
          from STUDENT
          where percentage between 40 and 100
        """.stripMargin

      val expectedDfCode =
        """STUDENT.
                           filter($"percentage".between(lit(40),lit(100))).
                            select($"student id").
                            distinct"""

      val dataFrame = new DataFrame(inputSql, DfName)
      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))

    }
  }

  "IN filter of sql" should {

    "convert IN (..,..,..,..) to isIN(Array(..,..,...,):_*) filter of dataframe" in {

      val inputSql =
        """select distinct student id
          from STUDENT
          where student IN (1,2,3,4)
        """.stripMargin

      val expectedDfCode =
        """STUDENT.
            filter($"student".isin(Array(1, 2, 3, 4):_*)).
             select($"student id").
             distinct"""

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))


    }

    "convert IN (..,..,..,..) to isIN(Array(..,..,...,):_*) and convert ' to \" filter of dataframe" in {

      val inputSql =
        """select distinct student_id
          from STUDENT
          where student IN ('1','2','3','4')
        """.stripMargin

      val expectedDfCode =
        """STUDENT.
            filter($"student".isin(Array("1","2", "3", "4"):_*)).
             select($"student_id").
             distinct"""

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))


    }

    "convert IN (SQL) to left_semi join of df representing (SQL)" in {

      val inputSql =
        """select distinct *
          from STUDENT
          where student IN (select student_id from FAILURE_LIST)
        """.stripMargin

      val expectedDfCode =
        """STUDENT.
           join(df_IsIn,"student" === "student_id","left_semi").
           select($"*").
           distinct"""

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))


    }
  }

  "Comparision filters of Sql" should {

    "convert != to =!= od dataFrame" in {

      val inputSql =
        """select distinct student_id
          from STUDENT
          where student != 123
        """.stripMargin

      val expectedDfCode =
        """STUDENT.
           filter($"student" =!= lit(123)).
            select($"student_id").
            distinct"""

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))

    }

    "convert = to === od dataFrame" in {

      val inputSql =
        """select distinct student_id
          from STUDENT
          where student = 123
        """.stripMargin

      val expectedDfCode =
        """STUDENT.
           filter($"student" === lit(123)).
            select($"student_id").
            distinct"""

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))

    }
  }

  "IS NOT NULL filter of Sql" should {
    "convert to .isNotNull of dataframe" in {

      val inputSql =
        """select distinct student_id
          from STUDENT
          where student is not null
        """.stripMargin

      val expectedDfCode =
        """STUDENT.
           filter($"student".isNotNull).
            select($"student_id").
            distinct"""

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))

    }

  }


}
