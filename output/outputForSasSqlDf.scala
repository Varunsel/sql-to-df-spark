
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PropensityScoring{
def main(args: Array[String]): Unit = {


val STUDENT_TEMP1 = student. 
 select($"student_id".as("id"),concat($"branch",$"college").as("branch_college")).
 distinct 
 
val STUDENT_TEMP = student. 
 select($"student_id".as("id"),concat($"weight",lit("_"),when($"weight".between(lit(0),lit(50)),lit("low")).
    otherwise(when($"weight".between(lit(51),lit(70)),lit("medium")).
    otherwise(when($"weight".between(lit(71),lit(100)),lit("high")).
    otherwise(lit("very high"))))).as("newweight")).
 distinct 
 
val STUDENT_TEMP2 = student. 
 select($"student_id".as("id"),trim(concat($"branch",lit("_"),translate(trim($"college"),lit(Regex),lit("")))).as("branch_college")).
 distinct 
 
val STUDENT_TEMP3 = student.
filter(concat(lit("cse"),lit("_"),$"college")  =!= $"branch_college"). 
 select($"student_id".as("id"),trim(concat($"branch",lit("_"),translate(trim($"college"),lit(Regex),lit("")))).as("branch_college")).
 distinct 
 
val STUDENT_TEMP4 = student. 
 select($"student_id".as("id"),when($"weight".between(lit(0),lit(50)),lit("low")).
    otherwise(when($"weight".between(lit(51),lit(70)),lit("medium")).
    otherwise(when($"weight".between(lit(71),lit(100)),lit("high")).
    otherwise(lit("very high")))).cast("char (64)").as("newweight")).
 distinct 
 
val STUDENT_TEMP5 = student. 
 select($"student_id".as("id"),trim(concat($"branch",lit("_"),translate(trim($"college"),lit(Regex),lit("")))).cast("char (64)").as("branch_college")).
 distinct 
 
val STUDENT_TEMP6 = student. 
 select($"student_id".as("id"),(concat($"branch",lit("_"),lit(postfix))).cast("char (64)").as("branch_college")).
 distinct 
 
val STUDENT_TEMP7 = student. 
 select($"student_id".as("id"),concat($"branch",$"college").as("branch_college")).
 distinct 
 
val STUDENT_TEMP8 = student. 
 select($"student_id".as("id"),concat($"branch",lit("_"),lit(postfix)).as("branch_college")).
 distinct 
 
}}