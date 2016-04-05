package example

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.{ SparkConf, TaskContext }
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.kafka.{ DirectKafkaInputDStream, HasOffsetRanges, OffsetRange }
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
      // auto offset reset is unfortunately necessary with dynamic topic subscription
      "auto.offset.reset" -> "latest"
    )
    val topics = conf.getString("kafka.topics").split(",")
    val ssc = new StreamingContext(new SparkConf, Milliseconds(conf.getLong("batchDurationMs")))
    val stream = DirectKafkaInputDStream[Array[Byte], Array[Byte]](
      ssc,
      DirectKafkaInputDStream.preferConsistent,
      kafkaParams.asJava,
      () => {
        // Set up the underlying consumer however you need to
        val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParams.asJava)
        consumer.subscribe(topics.toList.asJava)
        consumer
      }
    )

    stream.foreachRDD { rdd =>
      println(rdd.map(_.value.size).reduce(_+_))
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
