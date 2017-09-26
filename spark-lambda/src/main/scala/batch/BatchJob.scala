package batch

import java.lang.management.ManagementFactory
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import domain._

/**
  * Created by tomoakitsuruta on 9/24/17.
  */
object BatchJob {
  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Lambda with Spark")

    if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
      conf.setMaster("local[*]")
    }

    val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val sourceFile = "spark-test/data.tsv"
    val input = sc.textFile(sourceFile)



    // Spark.action

    val inputDF = input.flatMap { line =>
      val record = line.split("/t")
      val MS_IN_HOUR = 1000 * 60 * 60
      if (record.length == 7)
        Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6), record(7)))
      else
        None
    }.DF()

    sqlContext.udf.egister("UnderExposed", (pageViewCount: Long, purchaseCount: Long) => if (purchaseCount == 0) 0 else pageViewCount / purchaseCount)


    val df = inputDF.select(
      add_months(from_unixtime(inputDF("timestamp_hour") / 1000), 1).as("timestampe_hour"),
      inputDF("referrer"), inputDF("action"), inputDF("prevPage"), inputDF("page"), inputDF("visitor"), inputDF("product")
    ).cache()

    df.registerTempTable("activity")
    val visitorByProduct = sqlContext.sql(
      """SELECT product, timestamp_hour, COUNT(DISTINCT visitors) as unique_visitors
        |FROM acitivity GROUP BY product, timestamp_hour
      """.stripMargin
    )
    visitorByProduct.printSchema()

    val activityByProduct = sqlContext.sql(
      """SELECT
        |product,
        |timestamp_hour,
        |sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
        |sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
        |sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
        |from activity
        |group by product, timestamp_hour
        |
      """.stripMargin
    ).cache()

    activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs::/lambda-plualsight:9000/lambda/batch1")

    //activityByProduct.registerTempTable("activityByProduct")

    val underExposedProduct = sqlContext.sql(
      """SELECT,
        |product,
        |timestamp_hour,
        |UnderExposed(age_view_count, purchase_count) as negative_exposure,
        |from activityByProduct
        |order by negative_exposure DESC
        |limit 5
      """.stripMargin
    )



    val keyedByProduct = inputRDD.keyBy(a => (a.product, a.timestamp_hour)).cache()
    val visitorsByProduct = keyedByProduct
      .mapValues{ a => a.visitor }
      .distinct()
      .countByKey()


    val activityByProduct = keyedByProduct.mapValues{ a =>
      a.action.match {
        case "purchase" => (1, 0,0)
        case "add_to_cart" => (0,1,0)
        case "page_view" => (0,0,1)
      }
    }
      .reduceByKey( (a,b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
    visitorsByProduct.forEach(println)
    activityByProduct.forEach(println)




  }

}
