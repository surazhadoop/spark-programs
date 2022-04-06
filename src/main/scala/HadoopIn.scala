import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

object HadoopInputFormatProcessor {
  val inputFile = "/home/hduser/tempTSV.txt"
  val outputFile = "/home/hduser/tempTSV_output.txt"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    val inputRDD = sc.newAPIHadoopFile(inputFile, classOf[KeyValueTextInputFormat], classOf[Text], classOf[Text])
    inputRDD.foreach(println)

    val mapRDD = inputRDD.map {
      case (year, temp) => {
        println(s"$year  $temp")
        (year.toString, Integer.parseInt(temp.toString))
      }
    }

    mapRDD.collect.foreach(println)
    FileUtils.deleteQuietly(new File(outputFile))
    //mapRDD.saveAsNewAPIHadoopFile(outputFile, classOf[Text], classOf[IntWritable],classOf[TextOutputFormat[Text,IntWritable]])
    //mapRDD.saveAsTextFile(outputFile,classOf[org.apache.hadoop.io.compress.GzipCodec])

    sc.hadoopConfiguration.setClass(FileOutputFormat.COMPRESS_CODEC, classOf[org.apache.hadoop.io.compress.BZip2Codec], classOf[CompressionCodec])
    mapRDD.saveAsNewAPIHadoopFile(outputFile, classOf[Text], classOf[IntWritable],classOf[TextOutputFormat[Text,IntWritable]], sc.hadoopConfiguration)

    println("Program execute successfully")
  }
}
