import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

object SeqFileCreator {
  val inputFile = "/home/hduser/temp.txt"
  val outputFile = "/home/hduser/temp.seq"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    val yearTempRDD = sc.textFile(inputFile).map(line => {
      val arr = line.split(" ")
      (arr(0), Integer.parseInt(arr(1)))
    })

    FileUtils.deleteQuietly(new File(outputFile))
    yearTempRDD.saveAsSequenceFile(outputFile)

    println("Program executed successfully")
  }
}