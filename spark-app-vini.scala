/* Using spark shell to read github commits from 2015-01 
   I'll try to load this commits and pick up only the 
   commits related to hadoop projects
 */

 //https://hadoopsters.net/2017/09/01/how-to-write-orc-files-and-hive-partitions-in-spark/
 //https://stackoverflow.com/questions/42261701/how-to-create-hive-table-from-spark-data-frame-using-its-schema/45034949
 //https://spark.apache.org/docs/latest/sql-programming-guide.html#hive-tables
//export SPARK_MAJOR_VERSION=2
//spark-shell --master yarn --num-executors 4 --executor-cores 15 --deploy-mode client --executor-memory 30G

//Importing the datatypes to work with
import org.apache.spark.sql.types._

//Creating the SQLContext
val sqlconn = new org.apache.spark.sql.SQLContext(sc)

//Creating a dataframe to load the contents of the https://www.gharchive.org/
//All contents are in hdfs /user/spark/github_commits
val df_commits = sqlconn.read.json("/user/spark/spark_ds_github/2018/01/*")

//Let's print the schema first to get a clue of what we've
// df_commits.printSchema()
//Filtering only the repositories related to hadoop 
val hadoop = df_commits.filter($"repo.name".like("apache%"))
//Creating Database to allocate all orc files
//sqlconn.sql("create database spark_ds_github location '/user/hive/warehouse/spark_ds_github.db'")

//Creating the table partitioned by year/month
df_commits.createOrReplaceTempView("df")
/*val df_schema = df_commits.schema
val df_fields = df_schema.fields
var fieldStr = "";
for (StructType f <- df_schema.fields) {
fieldStr +=  f.name + " " + f.dataType.typeName + ",";
}*/
//drop the table if already created
//spark.sql("drop table if exists github_analysis");
//create the table using the dataframe schema
//sqlconn.setConf("hive.metastore.max.typename.length","100000") 
//val fields_string = fieldStr.substring(0,fieldStr.length()-1)
//sqlconn.sql(s"""CREATE EXTERNAL TABLE github_analysis(""" + fieldStr.substring(0,fieldStr.length()-1) + s""") PARTITIONED BY(year int, month int) STORED AS ORC LOCATION '/user/hive/warehouse/spark_ds_github.db' TBLPROPERTIES("orc.compress"="SNAPPY")""");
//sqlconn.sql(s"""CREATE EXTERNAL TABLE github_analysis( $fields_string ) PARTITIONED BY(year int, month int) STORED AS ORC LOCATION '/user/hive/warehouse/spark_ds_github.db' TBLPROPERTIES("orc.compress"="SNAPPY")""");
//val hive_table = sqlconn.sql("SELECT *, YEAR(created_at) as year, MONTH(created_at) as month  FROM df")

//Writing json files to hdfs path
//df_commits.write.orc("/user/hive/warehouse/spark_ds_github.db")

hive_table.filter($"repo.name".like("apache%")).write.partitionBy("year","month").option("orc.compress", "snappy").orc("/user/hive/warehouse/spark_ds_github.db/sparkds")

/*
Code apart from above
Function to flat StructType 1 level..

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import org.apache.spark.sql.Column;

def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
  schema.fields.flatMap(f => {
    val colName = if (prefix == null) f.name else (prefix + "." + f.name)

    f.dataType match {
      case st: StructType => flattenSchema(st, colName)
      case _ => Array(col(colName))
    }
  })
}

df_commits.select(flattenSchema(df_commits.schema):_*)
*/