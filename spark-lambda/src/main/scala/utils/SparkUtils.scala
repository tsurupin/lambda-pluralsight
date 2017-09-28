package utils

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
/**
  * Created by tomoakitsuruta on 9/26/17.
  */
object SparkUtils {

  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

  def getSparkContext(appName: String) = {


    var checkpointDirectory = ""

    val conf = new SparkConf().setAppName(appName)

    if (isIDE) {
      conf.setMaster("local[*]")
      checkpointDirectory = "temp"
    } else {
      checkpointDirectory = "hdfs://lambda-pluralsight/spark/checkpoint"
    }

    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkpointDirectory)
    sc
  }

  def getSQLContext(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sc
  }

  def getStreamingContext(streamingApp: (SparkContext, Duration) => StreamingContext, sc: SparkContext, batchDuration: Duration) = {
    val creatingFunc = () => streamingApp(sc, batchDuration)
    val ssc  = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }

    ssc.getCheckpointDir.forEach(cp => ssc.checkpoint(cp))
    ssc
  }

}
