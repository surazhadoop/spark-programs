import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
object WordCountDemo1 {
  val inputFile = "file:/home/hduser/word1.txt"
  val outputFilePath = new Path("hdfs://localhost:9000/user/hduser/output")

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("spark://ubuntu:7077").setAppName("My App")
      //.setJars(Seq("target/scala-2.12/wordcountsparkdemo_2.12-0.1.jar"))

    val sc = new SparkContext(conf)
    val inputRDD = sc.textFile(inputFile)
    println(s"Total Lines: ${inputRDD.count()}")

    val contentArr = inputRDD.collect()
    println("content:")
    contentArr.foreach(println)

    val words = inputRDD.flatMap(line => line.split(" "))
    val count1PerWords = words.map(word => (word, 1))
    val counts = count1PerWords.reduceByKey(_+_)
    val fs = outputFilePath.getFileSystem(sc.hadoopConfiguration)

    if (fs exists outputFilePath)
      fs.delete(outputFilePath, true)

    counts.saveAsTextFile(outputFilePath.toString)

    println("Program executed successfully")
  }
}
