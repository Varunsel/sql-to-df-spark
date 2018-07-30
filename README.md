###Aim : 

To convert most of the(not 100%) sql queries to Spark DataFrames to reduce copy paste work of developers.


###Flow
![Cat](https://github.com/mvamsichaitanya/sql-to-dataframe-generator/blob/master/src/main/resources/images/sql-to-dataframe-generator.jpg)


###How to use:

1. Clone the code from git


2. Edit the properties file in Config.scala(configs kept in scala file as user will be Data Engineer)


3. Edit SasSqlToSparkSql.scala and SqltoDf.scala to iterate each sql according to input.


4. Run the MainRun.scala class
