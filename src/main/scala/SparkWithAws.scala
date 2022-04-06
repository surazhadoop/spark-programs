import org.apache.spark.{SparkConf, SparkContext}

object SparkWithAWS {
  val inputFile = "s3a://olcbucket2/word1.txt"
  val outputFile = "s3a://olcbucket2/output/wordcount_output"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set("fs.s3a.access.key", "AKIATOELRFQPIJPDORPB")
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "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")
   val wordRDD = sc.textFile(inputFile)

    val words = wordRDD.flatMap(_.split("\\s+"))
    val wc = words.map(w => (w, 1)).reduceByKey(_ + _)
    wc.foreach(println)
    println("Read successfully")
    wc.saveAsTextFile(outputFile)
    println("Write successfully")
    println("Program execute successfully")
  }
}
