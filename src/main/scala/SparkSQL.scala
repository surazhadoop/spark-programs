import org.apache.spark.sql.SparkSession

object SparkSQLDemo1 {
  def main(args: Array[String]): Unit = {

    //create sparkSession by enabling Hive
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("SparkAndHive")
      .config("spark.sql.warehouse.dir","/tmp/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    //get all settings if you wish to
    val configMap = sparkSession.conf.getAll
    //Printing all the configuration setting
    configMap.foreach(println)

    //Read the json data directly to dataframe.
    val empDF = sparkSession.read.json("file:///home/hduser/data.json")
    //Create temporary view on the given dataframe
    empDF.cache()
    empDF.show()

    empDF.createOrReplaceTempView("employee")
    //This shows all the records of the dataframe.

    val resultsDF = sparkSession.sql("SELECT name, email[0], communication.phones.mobile FROM employee")
    resultsDF.show()

    sparkSession.sql("DROP TABLE IF EXISTS employee_details")
    //save as a hive table
    resultsDF.write.saveAsTable("employee_details33")

    val resultsHiveDF = sparkSession.sql("SELECT * FROM employee_details33")
    resultsHiveDF.show(10)
  }
}
