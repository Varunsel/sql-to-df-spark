
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PropensityScoring {
  def main(args: Array[String]): Unit = {


    val STUDENT_TEMP1 =
      spark.sql(
        s"""    select
        distinct student_id as id, 
 CONCAT(branch,college) as branch_college    from
    	student""")


    STUDENT_TEMP1.createOrReplaceTempView("STUDENT_TEMP1")


    val STUDENT_TEMP =
      spark.sql(
        s"""        select
         	distinct student_id as id, 
 CONCAT(weight,'_',(case
when weight between 0 and 50 then 'low'
when weight between 51 and 70 then 'medium'
when weight between 71 and 100 then 'high'
else 'very high'
end)) as newweight         from
           student""")


    STUDENT_TEMP.createOrReplaceTempView("STUDENT_TEMP")


    val STUDENT_TEMP2 =
      spark.sql(
        s"""    select
     	distinct student_id as id,trim(  
 CONCAT(branch,'_',translate(trim(college),Regex,''))) as branch_college     from
     	student""")


    STUDENT_TEMP2.createOrReplaceTempView("STUDENT_TEMP2")


    val STUDENT_TEMP3 =
      spark.sql(
        s"""            select distinct student_id as id,trim(
 CONCAT(branch,'_',translate(trim(college),Regex,''))) as branch_college        from
        	student
         where 
 CONCAT('cse','_',college) != branch_college""")


    STUDENT_TEMP3.createOrReplaceTempView("STUDENT_TEMP3")


    val STUDENT_TEMP4 =
      spark.sql(
        s"""        select
         	distinct student_id as id,         
 CAST((case
when weight between 0 and 50 then 'low'
when weight between 51 and 70 then 'medium'
when weight between 71 and 100 then 'high'
else 'very high'
end) as char(64) )  as newweight          from
           student""")


    STUDENT_TEMP4.createOrReplaceTempView("STUDENT_TEMP4")


    val STUDENT_TEMP5 =
      spark.sql(
        s""" select  distinct student_id as id,
 CAST(trim(
       CONCAT(branch,'_',translate(trim(college),Regex,''))) as char(64) )  as branch_college
          from
           student""")


    STUDENT_TEMP5.createOrReplaceTempView("STUDENT_TEMP5")


    val STUDENT_TEMP6 =
      spark.sql(
        s"""select
            distinct student_id as id,
         CAST((
            CONCAT(branch,'_','${postfix}')) as char(64) )  as branch_college
                 from
            student""")


    STUDENT_TEMP6.createOrReplaceTempView("STUDENT_TEMP6")


    val STUDENT_TEMP7 =
      spark.sql(
        s"""        select
         	distinct student_id as id, 
 CONCAT(branch,college) as ${branch_college}         from
           student""")


    STUDENT_TEMP7.createOrReplaceTempView("STUDENT_TEMP7")


    val STUDENT_TEMP8 =
      spark.sql(
        s"""        select
         	distinct student_id as id, 
 CONCAT(branch,'_','${postfix}') as ${branch_college}         from
           student""")


    STUDENT_TEMP8.createOrReplaceTempView("STUDENT_TEMP8")

  }
}