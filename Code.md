# loan-portfolio
# Analysis of Bank portfolio data for loan recommendations and data Preprocessing
import findspark
findspark.init()

findspark.find()

import os
print(os.environ['SPARK_HOME'])
print(os.environ['JAVA_HOME'])
print(os.environ['PATH'])

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

spark= SparkSession.builder.appName('Bank Loan Processing').getOrCreate()

# Load the Pending Status data file *which is a delimited file) into a dataframe
ps1=spark.read.load("C://Users//Priye Vatsal//Downloads//pendingstatus.tsv",format="csv",header=True,sep='\t',inferSchema=True)

ps1.count()

# Show sample records and check if all columns are displayed correctly
ps1.show()

ps1.printSchema()

ps1.dtypes

ps1.describe()

# Clean up by dropping all the records for which the most important columns have null value

ps1.na.drop()

ps1.createOrReplaceTempView("ps1")

ps2=spark.sql("select cust_id,yearmonth,date_modified,months_pending, \
int(substr(date_modified,7,2))+2000 as year_modified from ps1 where months_pending>=0")
ps2.count()

ps2.createOrReplaceTempView("ps2")

# Make sure you select the data of only the last 3 years from the given data set

ps3=spark.sql("select cust_id, months_pending, yearmonth, date_modified, year_modified from ps2 where year_modified >= (year_modified - 3)")
ps3.count()

ps3.createOrReplaceTempView("ps3")

# Perform the aggregations (count) of how many times the pending status was 0 and how many times it was 1 and then 2
# during the year for each loan application

# Note - You can use "CASE WHEN" statement with count aggregation to get the count of records when pending status is 0 or 1 or 2

ps4=spark.sql('select cust_id, year_modified, \
COUNT(CASE WHEN (months_pending = 0 ) then 1 ELSE NULL END) as cnt_ps_0, \
COUNT(CASE WHEN (months_pending = 1 ) then 1 ELSE NULL END) as cnt_ps_1, \
COUNT(CASE WHEN (months_pending = 2 ) then 1 ELSE NULL END) as cnt_ps_2 \
from ps3 group by cust_id, year_modified')
ps4.count()

ps4.createOrReplaceTempView("ps4")

# Load action records data into a dataframe
ar=spark.read.load("C://Users//Priye Vatsal//Downloads//actionrecords.tsv",format="csv",header=True,sep='\t',inferSchema=True)

ar.count()

ar.show(10)

# Take a look at the dataframe description and check descriptions or stats of all numeric columns are appripriate
# Note - You can use <df>.describe.show() function for this.
ar.describe()

# Take a look at the dataframe schema and check if data types of all columns are correctly inferred
ar.dtypes

ar.printSchema()

# Clean up by dropping all the records for which the most important columns have null value
# Note - you can use <df>.na.drop function for this giving the columns to checked for na or null values for dropping the rows

ar.na.drop()

ar.createOrReplaceTempView('ar')

# Notice that due to some invalid records in the data, datatype of some columns may not be inferred properly
# for example LAC_NO columns shown as string

ar1=spark.sql('select bigint(cust_id), date_of_action,\
int(from_unixtime(unix_timestamp(date_of_action,"dd-MM-yy"),"y")) as ar_year, action from ar')
ar1.count()

ar1.show(10)

ar1.printSchema()

ar1.createOrReplaceTempView('ar1')

# Drop the rows which have invalid i.e. incorrect type of data in important columns like LAC_NO etc.

ar2=ar1.na.drop()

# Show sample records and check if all columns are displayed correctly
ar2.show(5)

# Take a look at the dataframe schema and check if data types of all columns are correctly inferred
ar2.dtypes

ar2.createOrReplaceTempView('ar2')

# Make sure you select the data of only the last 3 years from the given data set

ar3=spark.sql("select cust_id, date_of_action, ar_year, action from ar2 where ar_year >= (ar_year - 3)")
ar3.count()

ar3.show(5)

ar3.createOrReplaceTempView('ar3')

# Join action records dataframe/temp view with pending status dataframe/temp view on customer id and year

ar4=spark.sql('select a.cust_id,a.date_of_action,a.ar_year,a.action,b.year_modified,b.cnt_ps_0,\
b.cnt_ps_1,b.cnt_ps_2 from ar3 a join ps4 b on a.cust_id=b.cust_id and a.ar_year=b.year_modified')
ar4.count()

ar4.show()

ar4.createOrReplaceTempView('ar4')

# Aggregate and get the number of times the following actions were taken up for the customers"
# - Telephone the customer (borrower) using filter or where condition - action like TB%
# - Telephone the coborrower using filter or where condition - action like TC%
# - Visit the customer (borrower) using filter or where condition - action like VB%
# - Telephone the coborrower using filter or where condition - action like VC%

ar5=spark.sql('select cust_id, ar_year,action, year_modified,cnt_ps_0,cnt_ps_1,cnt_ps_2, \
COUNT(CASE WHEN (action like "TB%" ) then 1 ELSE NULL END) as count_TB, \
COUNT(CASE WHEN (action like "TC%" ) then 1 ELSE NULL END) as count_TC, \
COUNT(CASE WHEN (action like "VB%" ) then 1 ELSE NULL END) as count_VB, \
COUNT(CASE WHEN (action like "VC%" ) then 1 ELSE NULL END) as count_VC \
from ar4 group by cust_id, ar_year,action, year_modified,cnt_ps_0,cnt_ps_1,cnt_ps_2 ')
ar5.count()

ar5.show()

ar5.createOrReplaceTempView('ar5')

# Load the customer details into a dataframe

lc=spark.read.load("C://Users//Priye Vatsal//Downloads//loandetails.tsv",format="csv",header=True,sep='\t',inferSchema=True)
lc.count()

lc.show(5)


lc.printSchema()

lc.createOrReplaceTempView('lc')

# Select only the required columns of the loan details
# and perform the usual verification of the data in the dataframe

lc1=spark.sql("select cust_id,loan_scheme,loan_amt,roi,emi,orig_term,loan_type,\
nri_flag,marital_status,sex,qualification1,emp_industry_type from lc")

lc1.show(5)

lc1.dtypes

# Take a look at the dataframe description and check descriptions or stats of all numeric columns are appripriate
# Note - You can use <df>.describe.show() function for this.
lc1.describe()

# Clean up by dropping all the records for which the most important columns have null value
# Note - you can use <df>.na.drop function for this giving the columns to checked for na or null values for dropping the rows
lc1.na.drop()

lc1.createOrReplaceTempView('lc1')

# For the final required output join the customer details dataframe with the previously generated dataframe
combined=spark.sql('select a.cust_id,a.loan_scheme,a.loan_amt,a.roi,a.emi,a.orig_term,a.loan_type,a.nri_flag,\
a.marital_status,a.sex,a.qualification1,a.emp_industry_type, b.year_modified,b.cnt_ps_0,b.cnt_ps_1,b.cnt_ps_2,\
b.count_TB,b.count_TC,b.count_VB,b.count_VC from lc1 a join ar5 b on a.cust_id=b.cust_id')

# From the combined dataframe of the above join operation
# For each loan each year list the loan details, customer details, number of times the pending status was 0, 1 and 2 months
# Followed by number of times actions like telephone calls and visits to the customer (borrower) and coborrower wer done 
combined.show(5)

combined.createOrReplaceTempView("combined")

# Save this final output dataframe records into a Hive table or into a tab-delimited file
combined.write.csv("C://Users//Priye Vatsal//Downloads//Loanreport.csv",sep='\t',header='true')


