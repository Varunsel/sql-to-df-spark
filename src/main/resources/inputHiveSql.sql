
    CREATE TABLE STUDENT_TEMP1 AS
      select distinct student id
          from STUDENT
          where percentage between 40 and 100;

    CREATE TABLE STUDENT_TEMP2 AS
       select distinct student id
          from STUDENT
          where student IN (1,2,3,4);

    CREATE TABLE STUDENT_TEMP3 AS
       select distinct student_id
          from STUDENT
          where student IN ('1','2','3','4');

    CREATE TABLE STUDENT_TEMP4 AS
      select distinct *
          from STUDENT
          where student IN (select student_id from FAILURE_LIST);

    CREATE TABLE STUDENT_TEMP5 AS
       select distinct student_id
          from STUDENT
          where student != 123;

    CREATE TABLE STUDENT_TEMP6 AS
       select distinct student_id
          from STUDENT
          where student = 123;

    CREATE TABLE STUDENT_TEMP7 AS
       select distinct student_id
          from STUDENT
          where student is not null;

    CREATE TABLE STUDENT_TEMP8 AS
    select  count(student_id)
          from STUDENT
          where student_id is not null;

    CREATE TABLE STUDENT_TEMP9 AS
    select  count(distinct student_id)
          from STUDENT
          where student_id is not null;

    CREATE TABLE STUDENT_TEMP10 AS
    select  trim(student_id) as student_id
          from STUDENT
          where student is not null;

    CREATE TABLE STUDENT_TEMP11 AS
    select  concat(first_name,' ',last_name) as student_full_name
          from STUDENT
          where student_id is not null;

    CREATE TABLE STUDENT_TEMP12 AS
    select  substr(student_id,0,4) as student_id
          from STUDENT
          where student_id is not null;


    CREATE TABLE STUDENT_TEMP13 AS
    select  student_id ,
           (maths + physics + chemistry) as total_marks
          from STUDENT
          where student_id is not null;

    CREATE TABLE STUDENT_TEMP14 AS
    select  student_id ,
           (((maths + physics + chemistry)/3)*100) as pct_marks
          from STUDENT
          where student_id is not null;

    CREATE TABLE STUDENT_TEMP15 AS
    select  cast(student_id as int) as student_id,
            (pass_dte - join_dte) as total_dys
          from STUDENT
          where student_id is not null;

    CREATE TABLE STUDENT_TEMP16 AS
    select  cast(student_id as int) as student_id,
            (pass_dte - 360) as sub_date
          from STUDENT
          where student_id is not null;

    CREATE TABLE STUDENT_TEMP17 AS
    select  cast(student_id as int) as student_id,
            (pass_dte + 360) as add_date
          from STUDENT
          where student_id is not null;


    CREATE TABLE STUDENT_TEMP18 AS
     select  cast(student_id as int) as student_id
          from STUDENT
          where student_id is not null;

           CREATE TABLE STUDENT_TEMP1 AS
                 SELECT
                          	DISTINCT student_id AS id,
                               concat(weight,'_',(CASE
                            WHEN WEIGHT BETWEEN 0 AND 50 THEN 'LOW'
                            WHEN WEIGHT BETWEEN 51 AND 70 THEN 'MEDIUM'
                            WHEN WEIGHT BETWEEN 71 AND 100 THEN 'HIGH'
                            ELSE 'VERY HIGH'
                            END)) AS NEWWEIGHT
                          FROM
                          	STUDENT;


           CREATE TABLE STUDENT_TEMP19 AS
                      SELECT
                          	DISTINCT student_id AS id,
                               concat(weight,'_',(CASE
                            WHEN WEIGHT BETWEEN 0 AND 50 THEN 'LOW'
                            WHEN WEIGHT BETWEEN 51 AND 70 THEN 'MEDIUM'
                            WHEN WEIGHT BETWEEN 71 AND 100 THEN 'HIGH'
                            END)) AS NEWWEIGHT
                          FROM
                          	STUDENT;

