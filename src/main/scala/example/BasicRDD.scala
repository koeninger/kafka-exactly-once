package example

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}
import com.typesafe.config.ConfigFactory

object BasicRDD {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val sc = new SparkContext(new SparkConf())

    val kafkaParams = Map("metadata.broker.list" -> conf.getString("kafka.brokers"))

    val topic = conf.getString("kafka.topics").split(",").toSet.head

    // change these values to offsets that actually exist for the topic
    val offsetRanges = Array(
      OffsetRange(topic, 0, 0, 100),
      OffsetRange(topic, 1, 0, 100)
    )

    val rdd = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](
      sc, kafkaParams, offsetRanges)

    rdd.collect.foreach(println)
    sc.stop
  }
}
