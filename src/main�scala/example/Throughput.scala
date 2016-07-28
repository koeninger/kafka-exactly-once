package example

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.{ SparkConf, TaskContext }
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ KafkaUtils, HasOffsetRanges, OffsetRange }
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import com.typesafe.config.ConfigFactory
import java.net.InetAddress
import scala.collection.JavaConverters._

object Throughput {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> conf.getString("kafka.brokers"),
      "key.deserializer" -> classOf[ByteArrayDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> "example",
      "auto.offset.reset" -> "latest"
    )
    val topics = conf.getString("kafka.topics").split(",")
    val ssc = new StreamingContext(new SparkConf, Milliseconds(conf.getLong("batchDurationMs")))
    val stream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
      ssc,
      PreferConsistent,
      Subscribe[Array[Byte], Array[Byte]](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      println(rdd.map(_.value.size.toLong).fold(0L)(_+_))
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
