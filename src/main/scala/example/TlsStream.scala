package example

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{ SparkConf, TaskContext }
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ KafkaUtils, HasOffsetRanges, OffsetRange }
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import com.typesafe.config.ConfigFactory
import java.net.InetAddress
import scala.collection.JavaConverters._

object TlsStream {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> conf.getString("kafka.brokers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "tlsexample",
      "auto.offset.reset" -> "earliest",
      // see the instructions at http://kafka.apache.org/documentation.html#security
      // make sure to change the port in bootstrap.servers if 9092 is not TLS
      "security.protocol" -> "SSL",
      "ssl.truststore.location" -> "/Users/cody/Downloads/kafka-keystore/kafka.client.truststore.jks",
      "ssl.truststore.password" -> "test1234",
      "ssl.keystore.location" -> "/Users/cody/Downloads/kafka-keystore/kafka.client.keystore.jks",
      "ssl.keystore.password" -> "test1234",
      "ssl.key.password" -> "test1234"
    )
    val topics = conf.getString("kafka.topics").split(",")
    val ssc = new StreamingContext(new SparkConf, Seconds(5))
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.mapPartitions { iter =>
        val osr: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        val host = InetAddress.getLocalHost().getHostName()
        val count = iter.size
        Seq(s"${host} ${osr.topic} ${osr.partition} ${count}").toIterator
      }.collect.sorted.foreach(println)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
