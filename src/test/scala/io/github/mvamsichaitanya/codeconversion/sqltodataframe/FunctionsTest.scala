package io.github.mvamsichaitanya.codeconversion.sqltodataframe

import io.github.mvamsichaitanya.codeconversion.TestCodeBase

class FunctionsTest extends TestCodeBase {

  final val DfName = "df"

  "count function in sql" should {

    "convert count function of dataFrame" in {

      val inputSql =
        """select  count(student_id)
          from STUDENT
          where student_id is not null
        """.stripMargin

      val expectedDfCode =
        """STUDENT.
         filter($"student_id".isNotNull).
          select(count($"student_id"))"""

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))

    }

    "convert count distinct function to countDistinct of dataFrame" in {
      val inputSql =
        """select  count(distinct student_id)
          from STUDENT
          where student_id is not null
        """.stripMargin

      val expectedDfCode =
        """STUDENT.
         filter($"student_id".isNotNull).
          select(countDistinct($"student_id"))"""

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))

    }
  }

  "single argument functions (trim,min,max,sum,sqrt) of the sql" should {

    "convert to data frame functions" in {

      val inputSql =
        """select  trim(student_id) as student_id
          from STUDENT
          where student is not null
        """.stripMargin

      val expectedDfCode =
        """STUDENT.
           filter($"STUDENT".isNotNull).
            select(trim($"student_id").as("student_id")) """

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))

    }

  }

  "multi argument functions (coalesce,concat,translate,substr,instr) of the sql" should {

    "convert to concat data frame function" in {

      val inputSql =
        """select  concat(first_name,' ',last_name) as student_full_name
          from STUDENT
          where student_id is not null
        """.stripMargin

      val expectedDfCode =
        """STUDENT.
           filter($"STUDENT_ID".isNotNull).
            select(concat($"first_name",lit(" "),$"last_name").as("student_full_name"))
        """

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))

    }

    "convert to substr data frame function" in {

      val inputSql =
        """select  substr(student_id,0,4) as student_id
          from STUDENT
          where student_id is not null
        """.stripMargin

      val expectedDfCode =
        """ STUDENT.
            filter($"STUDENT_ID".isNotNull).
             select(substr($"student_id",lit(0),lit(4)).as("student_id"))"""

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))

    }
  }

  "Arithmetic functions" should {

    "processed to data frame specific" in {

      val inputSql =
        """select  student_id ,
           (maths + physics + chemistry) as total_marks
          from STUDENT
          where student_id is not null
        """.stripMargin

      val expectedDfCode =
        """ STUDENT.
           filter($"STUDENT_ID".isNotNull).
            select($"student_id",($"maths" + $"physics" + $"chemistry").as("total_marks"))"""

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))

    }

    "process multiple arithmetic functions to data frame specific" in {

      val inputSql =
        """select  student_id ,
           (((maths + physics + chemistry)/3)*100) as pct_marks
          from STUDENT
          where student_id is not null
        """.stripMargin

      val expectedDfCode =
        """ STUDENT.
           filter($"STUDENT_ID".isNotNull).
            select($"student_id",((($"maths" + $"physics" + $"chemistry") / lit(3)) * lit(100)).as("pct_marks"))"""

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))

    }

  }

  "arithmetic function with date columns" should {
    "convert '-' to datediff if both arguments are date functions" in {

      val inputSql =
        """select  cast(student_id as int) as student_id,
            (pass_dte - join_dte) as total_dys
          from STUDENT
          where student_id is not null
        """.stripMargin

      val expectedDfCode =
        """STUDENT.
           filter($"STUDENT_ID".isNotNull).
            select($"student_id".cast("int").as("student_id"),datediff($"pass_dte",$"join_dte").as("total_dys")) """

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))
    }

    "convert '-' to date_sub if second argument is number and first arg is date" in {

      val inputSql =
        """select  cast(student_id as int) as student_id,
            (pass_dte - 360) as sub_date
          from STUDENT
          where student_id is not null
        """.stripMargin

      val expectedDfCode =
        """STUDENT.
           filter($"STUDENT_ID".isNotNull).
            select($"student_id".cast("int").as("student_id"),date_sub($"pass_dte",lit(360)).as("sub_date"))"""

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))
    }

    "convert '+' to date_add if second argument is number and first arg is date" in {

      val inputSql =
        """select  cast(student_id as int) as student_id,
            (pass_dte + 360) as add_date
          from STUDENT
          where student_id is not null
        """.stripMargin

      val expectedDfCode =
        """STUDENT.
           filter($"STUDENT_ID".isNotNull).
            select($"student_id".cast("int").as("student_id"),date_add($"pass_dte",lit(360)).as("add_date"))"""

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))
    }
  }


  "cast function of sql" should {

    "convert to .cast of data frame" in {

      val inputSql =
        """select  cast(student_id as int) as student_id
          from STUDENT
          where student_id is not null
        """.stripMargin

      val expectedDfCode =
        """ STUDENT.
          filter($"STUDENT_ID".isNotNull).
           select($"student_id".cast("int").as("student_id"))"""

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))

    }
  }

  "case when conditions" should {

    "convert to when otherwise functions of data frame" in {

      val inputSql =
        """SELECT
                          	DISTINCT student_id AS id,
                               concat(weight,'_',(CASE
                            WHEN WEIGHT BETWEEN 0 AND 50 THEN 'LOW'
                            WHEN WEIGHT BETWEEN 51 AND 70 THEN 'MEDIUM'
                            WHEN WEIGHT BETWEEN 71 AND 100 THEN 'HIGH'
                            ELSE 'VERY HIGH'
                            END)) AS NEWWEIGHT
                          FROM
                          	STUDENT"""

      val expectedDfCode = """STUDENT.
                              select($"student_id".as("id"),concat($"weight",lit("_"),
                              when($"WEIGHT".between(lit(0),lit(50)),lit("LOW")).
                                 otherwise(when($"WEIGHT".between(lit(51),lit(70)),lit("MEDIUM")).
                                  otherwise(when($"WEIGHT".between(lit(71),lit(100)),lit("HIGH")).
                                   otherwise(lit("VERY HIGH"))))).as("NEWWEIGHT")).
                              distinct"""



      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))

    }

    "convert to when otherwise functions of data frame. case when without ELSE" in {

      val inputSql =
        """SELECT
                          	DISTINCT student_id AS id,
                               concat(weight,'_',(CASE
                            WHEN WEIGHT BETWEEN 0 AND 50 THEN 'LOW'
                            WHEN WEIGHT BETWEEN 51 AND 70 THEN 'MEDIUM'
                            WHEN WEIGHT BETWEEN 71 AND 100 THEN 'HIGH'
                            END)) AS NEWWEIGHT
                          FROM
                          	STUDENT"""

      val expectedDfCode = """STUDENT.
                               select($"student_id".as("id"),concat($"weight",lit("_"),
                               when($"WEIGHT".between(lit(0),lit(50)),lit("LOW")).
                                  otherwise(when($"WEIGHT".between(lit(51),lit(70)),lit("MEDIUM")).
                                  otherwise(when($"WEIGHT".between(lit(71),lit(100)),lit("HIGH"))))).as("NEWWEIGHT")).
                               distinct"""

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))

    }
  }

  "nested functions of SQL" should {

    "convert to data frame functions" in {

      val inputSql =
        """select  count(trim(concat(translate(student_id,'^%&*$',''),'_',name))) as new_id
          from STUDENT
          where student_id is not null
        """.stripMargin

      val expectedDfCode =
        """STUDENT.
           filter($"STUDENT_ID".isNotNull).
            select(count(trim(concat(translate($"student_id",lit("^%&*$"),lit("")),lit("_"),$"name"))).as("new_id")) """

      val dataFrame = new DataFrame(inputSql, DfName)

      val outputDfCode = dataFrame.dataFrameCode

      assert(compareDataFrame(expectedDfCode, outputDfCode))

    }


  }



}
