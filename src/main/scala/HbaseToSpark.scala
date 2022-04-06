/*

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}

import java.util
import scala.collection.JavaConverters._

case class Employee(empId:String,empName:String,empSalary:String,lane:String,pin:String,street:String)

object HBaseToSpark {
  def main(args: Array[String]): Unit = {
    val hbaseConfig = HBaseConfiguration.create
    hbaseConfig.set("hbase.zookeeper.quorum", "localhost")
    hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConfig.set(TableInputFormat.INPUT_TABLE, "employee") // which table to scan

    val conf = new SparkConf().setMaster("local").setAppName("Spark From Hbase")
    val sc = new SparkContext(conf)

    val rdd = sc.newAPIHadoopRDD(hbaseConfig, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val empRDD=rdd.map{case(key,value)=>{

      val empId = value.getValue(Bytes.toBytes("basic"), Bytes.toBytes("empId"))
      val empName = value.getValue(Bytes.toBytes("basic"), Bytes.toBytes("empName"))
      val empSalary = value.getValue(Bytes.toBytes("basic"), Bytes.toBytes("empSalary"))
      val lane = value.getValue(Bytes.toBytes("address"), Bytes.toBytes("Lane"))
      val pin = value.getValue(Bytes.toBytes("address"), Bytes.toBytes("pincode"))
      val street = value.getValue(Bytes.toBytes("address"), Bytes.toBytes("street"))

      println("empID:" + Bytes.toString(empId))
      println("empName:" + Bytes.toString(empName))
      println("empSalary:" + Bytes.toString(empSalary))
      println("lane:" + Bytes.toString(lane))
      println("pin:" + Bytes.toString(pin))
      println("street:" + Bytes.toString(street))
      Employee(Bytes.toString(empId),Bytes.toString(empName),Bytes.toString(empSalary),Bytes.toString(lane),Bytes.toString(pin),Bytes.toString(street))
    }}
    empRDD.foreach(println)

   empRDD.foreachPartition( records => {
     val conf = HBaseConfiguration.create();
     val connection = ConnectionFactory.createConnection(conf);
     val table = connection.getTable(TableName.valueOf("filtered_employee"));

     val putList: util.List[Put] = records.map(record => {
        val put = new Put(Bytes.toBytes(record.empId))
        put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("empId"), Bytes.toBytes(record.empId))
        put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("empName"), Bytes.toBytes(record.empName))
        put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("empSalary"), Bytes.toBytes(record.empSalary))
        put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("Lane"), Bytes.toBytes(record.lane))
        put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("pincode"), Bytes.toBytes(record.pin))
        put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("street"), Bytes.toBytes(record.street))
        put
      }).toList.asJava
     table.put(putList)
     println("done")
      table.close()
    })
  }}

*/
