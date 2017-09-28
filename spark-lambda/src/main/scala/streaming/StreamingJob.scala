package streaming
import com.twitter.algebird.HyperLogLogMonoid
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.SparkUtils._
import functions._
/**
  * Created by tomoakitsuruta on 9/26/17.
  */
object StreamingJob {

  def main(args: Array[String]) : Unit = {

    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)
    import sqlContext.implicits._

    val batchDuration = Seconds(4)

    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)
      val wlc = Settings.WebLogin
      val topic = wlc.kafkaTopic

      val kafkaDirectParams = Map(
        "metadata.broker.list" -> "localhost: 9092",
        "group.id" -> "lambda",
        "auto.offset.reset" -> "largest"
      )

      val kafkaDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaDirectParams, Set(topic)
      )

      val acitivityStream = kafkaDirectStream.transform(input => {
        functions.rddToRDDActivity(input)
      }).cache()

      acitivityStream.foreachRDD{ rdd =>
        val acitivityDF = rdd.toDF().selectEcpr("timestamp_hour", "referrer", "action",
          "prevPage", "page", "visitor", "product", "inputProps.topic as topic", "inputProps.kafkaPartition as kafkaPartition", "inputProps.fromOffset as fromOffset", "inputProps.untilOffset as untillOffset")
        acitivityDF.write.partitionBy("topic", "kafkaPartition", "timestamp_hour").mode(SaveMode.Append).parquet("hdfs://lambda-pluralsight:9000/lambda/weblogs-appl/")
        activityDF.write.partitionBy()
      }

      val inputPath = isIDE match {
        case true => "file://spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
        case false => "file://vagrant/input"
      }

      val textDStream = ssc.textFileStream(inputPath)

      val activityStream = textDStream.transform( input => {
        input.flatMap{ line =>
          val record = line.split("/t")
          val MS_IN_HOUR = 1000 * 60 * 60
          if (record.length == 7) {
            Some(Activity(record(0).toLong /MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(2), record(3), record(4), record(5), record(6)))
          } else
            None
        }
      }).cache()

      val aciticityStateSpec =
        StateSpec.function(mapActivityStateFunc)
        .timeout(Minutes(120))

      val statefulActivityByProduct = activityStream.transform ( rdd => {
        val df = rdd.toDF()
        df.registerTempTable("activity")
        val activityByProduct = sqlContext.sql( """SELECT
                                                  |product,
                                                  |timestamp_hour,
                                                  |sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                                  |sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                                  |sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                                  |from activity
                                                  |group by product, timestamp_hour
                                                  |""".stripMargin)
        activityByProduct.map { r => ((r.getString(0), r.getLong(1)),
        ActivityByProduct(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4))
        )}

      }).mapWithState(activityStateSpec)

      val acitivityStateSnapshot = statefulActivityByProduct.stateSnapshots()
      acitivityStateSnapshot.reduceByKeyAndWindow(
        (a, b) => b,
        (x, y) => x,
        Seconds(30 / 4 * 4)
      ).map(sr => ActivityByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))
        .saveToCassandra("lambda", "stream_activity_by_product")


      val visitorStateSpec = StateSpec.function(mapVisitorsStateFunc)
        .timeout(Minutes(120))

      val hll = new HyperLogLogMonoid(12)

      val statefulVisitorsByProduct = activityStream.map( a => {
        ((a.product, a.timestamp_hour, a.visitor.getBytes))

      }).mapWithState(visitorStateSpec)

      val visitorStateSnapshot = statefulVisitorsByProduct.stateSnapshots()
      visitorStateSnapshot.reduceByKeyAndWindow(
        (a, b) => b,
        (x, y) => x,
        Seconds(30 / 4 * 4)
      ).map(sr => VisitorsByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))
        .saveToCassandra("lambda", "VisitorsByProduct"))


      ssc

    }


    val ssc = getStreamingContext(streamingApp, sc, batchDuration)

    ssc.start()
    ssc.awaitTermination()
  }

}
