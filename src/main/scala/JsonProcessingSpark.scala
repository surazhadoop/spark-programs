/*
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule, ScalaObjectMapper}
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

case class Employee(name: String, dob: String, salary: Int, email: List[String], communication: Communication)
case class Communication(phones: Map[String, String], address: Address)
case class Address(area: String, district: String, pin: Int)

object JsonProcessing {
  val inputFile = "/home/hduser/data.json"
  val outputFile = "/home/hduser/jsonOutput.txt"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    val inputRDD = sc.textFile(inputFile)
    FileUtils.deleteQuietly(new File(outputFile))


    //For all the reords,convert the json to Employee object and filter those records where employee_salary >10000
    val empSalGT10kRDD = inputRDD.mapPartitions(records => {
      // mapper object created on each executor node
      val mapper = new ObjectMapper with ClassTagExtensions
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)

      records.flatMap(record => {
        try {
          Some(mapper.readValue(record, classOf[Employee]))
        } catch {
          case e: Exception => None
        }
      }).filter(employee => employee.salary > 10000)
    }, true)

    println(empSalGT10kRDD.count())

    empSalGT10kRDD.foreach(emp => println(emp))

    //This is to write the data into the file.
    empSalGT10kRDD.mapPartitions(records => {
      val mapper = new ObjectMapper with ClassTagExtensions
      mapper.registerModule(DefaultScalaModule)
      records.map(mapper.writeValueAsString(_))
    }).saveAsTextFile(outputFile)


    println("Program executed successfully")
  }
}
*/
