import org.apache.spark.sql.SparkSession

object SparkSQLDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("SparkAndHive")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse2")
      .enableHiveSupport()
      .getOrCreate()

    case class Employee(name:String,age:Int,salary:Int,email:String,deptName:String)
    val empList = Seq(Employee("Suraj", 29, 5000, "suraz.hadoop@gmail.com", "IT"),
      Employee("Isha", 27, 6000, "isha.oracle@gmail.com", "IT"),
      Employee("Karan", 44, 20000, "karan.java@gmail.com", "HR"),
      Employee("Kiran", 41, 22000, "kiran.scala@gmail.com", ""),
      Employee("Kumar", 44, 25000, "kumar.scala@gmail.com",""))


    import sparkSession.implicits._
    //val empDF = sparkSession.createDataFrame(empList)//("name", "age", "salary", "email", "dept")
    //empDF.show


  }
}