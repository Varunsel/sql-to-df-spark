    CREATE TABLE STUDENT_TEMP1 AS
    SELECT
        DISTINCT student_id AS id,
        branch||college AS branch_college
    FROM
    	STUDENT;

  CREATE TABLE STUDENT_TEMP AS
        SELECT
         	DISTINCT student_id AS id,
              weight||'_'||(CASE
           WHEN WEIGHT BETWEEN 0 AND 50 THEN 'LOW'
           WHEN WEIGHT BETWEEN 51 AND 70 THEN 'MEDIUM'
           WHEN WEIGHT BETWEEN 71 AND 100 THEN 'HIGH'
           ELSE 'VERY HIGH'
           END) AS NEWWEIGHT
         FROM
           (SELECT * FROM STUDENT WHERE student_id is not null) a;

    CREATE TABLE STUDENT_TEMP2 AS
    SELECT
     	DISTINCT student_id AS id,
     	trim( branch||'_'||translate(trim(college),' &/!@#$%^*;?<>":=+-~`\|[]{}_,.()''','')) AS branch_college
     FROM
     	student;



    CREATE TABLE STUDENT_TEMP3 AS
            SELECT DISTINCT student_id AS id,
        	trim( branch||'_'||translate(trim(college),' &/!@#$%^*;?<>":=+-~`\|[]{}_,.()''','')) AS branch_college
        FROM
        	student
         where
         'CSE'||'_'||college != branch_college;


         SELECT
          	DISTINCT student_id :: int AS id
          FROM
            STUDENT;



        CREATE TABLE STUDENT_TEMP4 AS
        SELECT
         	DISTINCT student_id AS id,
        (CASE
           WHEN WEIGHT BETWEEN 0 AND 50 THEN 'LOW'
           WHEN WEIGHT BETWEEN 51 AND 70 THEN 'MEDIUM'
           WHEN WEIGHT BETWEEN 71 AND 100 THEN 'HIGH'
           ELSE 'VERY HIGH'
           END) :: char(64) AS NEWWEIGHT
         FROM
           STUDENT;

        CREATE TABLE STUDENT_TEMP5 AS
        SELECT
             	DISTINCT student_id AS id,
             	trim( branch||'_'||translate(trim(college),' &/!@#$%^*;?<>''','')) :: char(64) AS branch_college
             FROM
               STUDENT;

        CREATE TABLE STUDENT_TEMP6 AS
        SELECT
         	DISTINCT student_id AS id,
              (branch||'_'||%str(%')&postfix%str(%')) :: char(64) AS branch_college
         FROM
           STUDENT;

        CREATE TABLE STUDENT_TEMP7 AS
        SELECT
         	DISTINCT student_id AS id,
              branch||college AS &branch_college
         FROM
           STUDENT;


        CREATE TABLE STUDENT_TEMP8 AS
        SELECT
         	DISTINCT student_id AS id,
              branch||'_'||%str(%')&postfix%str(%') AS &branch_college
         FROM
           STUDENT;

