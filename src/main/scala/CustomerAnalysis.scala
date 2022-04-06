import org.apache.spark.{SparkConf, SparkContext}

object CustomerAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    val customerDetailFile = "/home/hduser/customerDetails/customerDetails.txt"
    val customerBalanceFile="/home/hduser/customerDetails/customerBalance.txt"

    //Loads the file into RDD
    val detailsRDD = sc.textFile(customerDetailFile)
    val balanceRDD= sc.textFile(customerBalanceFile)

    detailsRDD.collect().foreach(println)
    balanceRDD.collect().foreach(println)

    val idDetailsKeyValue=detailsRDD.map(row=>{
      //SBI10001,Suraj,Ghimire
      val data=row.split(",")
      (data(0),data(1)+" "+data(2))
      //SBI10001,Suraj Ghimire

    })
    idDetailsKeyValue.foreach(println)

    val idBalanceKeyValue=balanceRDD.map(row=>{
      //SBI10001,50000
      val data=row.split(",")
      (data(0),data(1))
    })

    idBalanceKeyValue.foreach(println)
    val joined=idDetailsKeyValue.join(idBalanceKeyValue).sortByKey()
    joined.collect.foreach(println)

    val finalOutput=joined.map{case(accno, (name, amount)) => (accno, name,amount)}
    finalOutput.collect.foreach(println)



    val pairedByBalance=finalOutput.map{
      case(accno,name,amount)=>
        (amount.toFloat,(accno,name))
    }
    pairedByBalance.foreach(println)

    val sortedByBalance = pairedByBalance.sortByKey(false)
    sortedByBalance.foreach(println)

    val highestBalance=sortedByBalance.first(); //This doesnot return RDD
    println(highestBalance)

    println(highestBalance._2+" "+highestBalance._1)
    println(highestBalance._2._1+" "+highestBalance._2._2+" "+highestBalance._1)


    val top3BalanceCustomer=sortedByBalance.top(3) //Returns you an Array
    top3BalanceCustomer.foreach(println)

    val formattedTop3=top3BalanceCustomer.map{
      case(balance,(accNo,name))=>(accNo,name,balance)
    }
    formattedTop3.foreach(println)
    idBalanceKeyValue.collect.foreach(println)
    val onlyBalanceRDD=idBalanceKeyValue.map{case(_,balance)=>balance.toFloat}
    println("Total Balance :"+onlyBalanceRDD.sum())

    val balanceTotal = sc.longAccumulator("Account Balance Total")
    idBalanceKeyValue.foreach(data =>balanceTotal.add(data._2.toInt))
    println("Using accumulator "+balanceTotal.value)


  }

}
