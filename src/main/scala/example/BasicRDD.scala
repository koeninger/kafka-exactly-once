package example

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka010.{ KafkaUtils, OffsetRange }
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import scala.collection.JavaConverters._
import com.typesafe.config.ConfigFactory

object BasicRDD {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> conf.getString("kafka.brokers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer]
    ).asJava

    val sc = new SparkContext(new SparkConf())

    val topic = conf.getString("kafka.topics").split(",").toSet.head

    // change these values to offsets that actually exist for the topic
    val offsetRanges = Array(
      OffsetRange(topic, 0, 0, 100),
      OffsetRange(topic, 1, 0, 100)
    )

    val rdd = KafkaUtils.createRDD[String, String](sc, kafkaParams, offsetRanges, PreferConsistent)

    rdd.collect.foreach(println)
    sc.stop
  }
}
