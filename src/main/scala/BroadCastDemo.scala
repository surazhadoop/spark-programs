/*


import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class Employee(empNo: Int, empName: String, empJob: String, empSalary: Float, empMgr: String, deptNo: Int)

case class Department(deptNo: Int, deptName: String, deptLoc: String)

object BroadCastDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    val empFileRDD = sc.textFile("/home/hduser/empDept/employee.txt", 2)
    val deptFileRDD = sc.textFile("/home/hduser/empDept/dept.txt", 2)

    empFileRDD.foreach(println)
    deptFileRDD.foreach(println)
    //111,Saketh,Analyst,6000,444,10
    val empRDD:RDD[Employee] = empFileRDD.map(emp => {
      val data = emp.split(",")
      Employee(data(0).toInt, data(1), data(2), data(3).toFloat, data(4), data(5).toInt)
    })

    val deptRDD:RDD[(Int,Department)] = deptFileRDD.map(dept => {
      val data = dept.split(",")
      (data(0).toInt, Department(data(0).toInt, data(1), data(2)))
    })

    val broadcastedDept: Broadcast[collection.Map[Int, Department]] = sc.broadcast(deptRDD.collectAsMap())
    //sc.broadcast()


    val empDeptRDD = empRDD.mapPartitions({ partition => {
      val deptMap=broadcastedDept.value
      partition.map(emp => {
        val dept = deptMap.getOrElse(emp.deptNo, null)
        if (dept != null) {
          (emp.empNo, emp.empName, emp.empJob, emp.empMgr, emp.empSalary, dept.deptName, dept.deptLoc)
        }
      })
    }
    }, preservesPartitioning = true)

    empDeptRDD.foreach(println)


  }
}

*/
