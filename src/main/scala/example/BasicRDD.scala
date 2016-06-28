package example

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.{ KafkaUtils, OffsetRange, PreferConsistent }
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

    val offsetRanges = Array(
      OffsetRange("test", 0, 0, 100),
      OffsetRange("test", 1, 0, 100)
    )

    val rdd = KafkaUtils.createRDD[String, String](sc, kafkaParams, offsetRanges, PreferConsistent)

    rdd.collect.foreach(println)
    sc.stop
  }
}
