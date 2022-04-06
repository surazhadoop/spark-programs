import java.util.UUID
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object WordCount {
  val inputFile = "file:/home/hduser/word1.txt"
  val outputFilePath = new Path("hdfs://localhost:9000/user/hduser/output")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("yarn").setAppName("My App")
    conf.set("spark.executor.memory","1g")

    val sc = new SparkContext(conf)
    val inputRDD = sc.textFile(inputFile)
    println(s"Total Lines: ${inputRDD.count()}")

    val contentArr = inputRDD.collect()
    println("content:")
    contentArr.foreach(println)

    val words = inputRDD.flatMap(line => line.split(" "))
    val count1PerWords = words.map(word => (word, 1))
    val counts: RDD[(String, Int)] = count1PerWords.reduceByKey { case (counter, nextVal) =>	 counter + nextVal }

    val fs = outputFilePath.getFileSystem(sc.hadoopConfiguration)

    if (fs exists outputFilePath)
      fs.delete(outputFilePath, true)

    counts.collect.foreach(println)
    counts.saveAsTextFile(outputFilePath.toString)
    println("Program executed successfully")
  }
}
