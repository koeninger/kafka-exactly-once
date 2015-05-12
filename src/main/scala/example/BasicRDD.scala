package example

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}

object BasicRDD {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf())

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092,anotherhost:9092")

    val offsetRanges = Array(
      OffsetRange("test", 0, 0, 100),
      OffsetRange("test", 1, 0, 100)
    )

    val rdd = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](
      sc, kafkaParams, offsetRanges)

    rdd.collect.foreach(println)
    sc.stop
  }
}
