package example

import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer }
import org.apache.kafka.clients.consumer.KafkaConsumer
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._

// direct usage of the KafkaConsumer
object BasicKafkaConsumer {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> conf.getString("kafka.brokers"),
      "key.deserializer" -> classOf[ByteArrayDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> "example",
      "receive.buffer.bytes" -> (65536: java.lang.Integer),
      // auto offset reset is unfortunately necessary with dynamic topic subscription
      "auto.offset.reset" -> "latest"
    ).asJava
    val topics = conf.getString("kafka.topics").split(",").toList.asJava
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParams)
    consumer.subscribe(topics)
    consumer.poll(0)
    println("Starting positions are: ")
    consumer.assignment.asScala.foreach { tp =>
      println(s"${tp.topic} ${tp.partition} ${consumer.position(tp)}")
    }
    while (true) {
      println(consumer.poll(512).asScala.map(_.value.size.toLong).fold(0L)(_+_))
      Thread.sleep(1000)
    }
  }
}
