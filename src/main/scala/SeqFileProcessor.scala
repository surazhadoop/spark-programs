import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}

object SeqFileProcessor {
  val inputFile = "/home/hduser/temp.seq/part-00000"
  val outputFile = "/home/hduser/temp_output"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    val inputRDD = sc.sequenceFile(inputFile, classOf[org.apache.hadoop.io.Text], classOf[IntWritable])

    val yearTempRDD = inputRDD.map { case (year, temp) => {
      println(s"$year $temp")
      //You need to convert them to Scala type from hadoop Type,Else you get serialization issue.It is working in current release
      //(year.toString, temp.get())
      (year, temp) //This also works now.

    }
    }
    FileUtils.deleteQuietly(new File(outputFile))

    yearTempRDD.saveAsSequenceFile(outputFile)

    println("Program executed successfully")
  }
}
