import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

object ObjectFileCreator {
  val outputFile = "/home/hduser/object.txt"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    val numRDD = sc.parallelize(1 to 10 toList, 2)
    val arr = Array("Even", "Odd")

    val mapRDD = numRDD.map(num => (arr(num % 2), num))
    mapRDD.collect.foreach(println)

    FileUtils.deleteQuietly(new File(outputFile))
    mapRDD.saveAsObjectFile(outputFile)
    println("Object file written successfully")

    println("Reading Object file")
    val dataRDD = sc.objectFile(s"${outputFile}/part*", 2)
    dataRDD.foreach(println)

    println("Program executed successfully")
  }
}
