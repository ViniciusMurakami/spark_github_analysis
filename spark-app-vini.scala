/* Using spark shell to read github commits from 2015-01 
   I'll try to load this commits and pick up only the 
   commits related to hadoop projects
 */

//export SPARK_MAJOR_VERSION=2
//spark-shell --master yarn

//Creating the SQLContext
 val sqlconn = new org.apache.spark.sql.SQLContext(sc);

//Creating a dataframe to load the contents of the https://www.gharchive.org/
//All contents are in hdfs /user/spark/github_commits
 val df_commits = sqlconn.read.json("/user/spark/github_commits/*/*/*")

//Let's print the schema first to get a clue of what we've
 df_commits.printSchema()
//Filtering only the repositories related to hadoop 
 val hadoop = df_commits.filter($"repo.name".like("apache%hadoop"))
//Writing json files to hdfs path
 hadoop.write.json("/user/spark/hadoop")





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