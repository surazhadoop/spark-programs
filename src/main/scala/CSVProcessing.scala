/*
import java.io.{File, StringReader, StringWriter}

import com.opencsv.{CSVReader, CSVWriter}
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

case class Employee(name:String,age:String,salary:String,address:String,email:String,phone:String)

object CSVProcessing {
  val inputFile = "/home/hduser/emp.csv"
  val outputFile = "/home/hduser/empOutput.txt"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    val inputRDD = sc.textFile(inputFile)

    FileUtils.deleteQuietly(new File(outputFile))

    val skipFirstLineRDD=inputRDD.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }

    val empRDD = skipFirstLineRDD.map{ line =>
      val reader = new CSVReader(new StringReader(line));
      val data=reader.readNext() //suraj,29,5000, Hyd, suraz.hadoop@gmail.com,9032412236
      Employee(data(0),data(1),data(2),data(3),data(4),data(5))
    }
     //val filteredRdd=empRDD.filter(_.salary.toInt>5000)
     //Write some Business logic
     //Start writing it back
     val rddArrString=empRDD.map(employee => List(employee.name, employee.age,employee.salary).toArray)

    val addHeaderRDD=rddArrString.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) Iterator(Array("Name","Age","Salary"))++iter else iter
    }

    val finalResult=addHeaderRDD.mapPartitions{it =>
      val stringWriter = new StringWriter()
      val csvWriter = new CSVWriter(stringWriter)

      while(it.hasNext) {
        val data=it.next() //returning you back an Array[String]
        println("data="+data.mkString("|"))
        csvWriter.writeNext(data)
      }
      Iterator(stringWriter.toString)
    }
    finalResult.saveAsTextFile(outputFile)

    //This is to write the data into the file.
    //result.saveAsTextFile(outputFile)

    println("Program executed successfully")
  }
}

*/
