

##Aim : 

To convert most of the(not 100%) sql queries to Spark DataFrames to reduce copy paste work of developers.


##Supports:

Nested Queries
Nested Functions
complex Arithmetic functions with date columns
filters:


###functions:



case when functions


todo:

Implement Window function

Queries like column == select max(otherColumn) from table



###Make Sure following rules are followed:



Executable code with no create table statement of following format

=> Create table table_1(column_1 int,
                      column_2 bigint,
                        column_3 string);

=> No Union all
 
=> No not regexp_like

=> IN SAS Sql if || or :: functions are used then as or alias is must


Supported sql's:

Sas Sql and Hive Sql

Process:

Sas Sql
   |
Plain sql
   |
Spark sql
   |
Spark DataFrame