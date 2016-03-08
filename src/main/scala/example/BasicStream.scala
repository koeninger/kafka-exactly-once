package example

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{ SparkConf, TaskContext }
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{ DirectKafkaInputDStream, HasOffsetRanges, OffsetRange }
import com.typesafe.config.ConfigFactory
import java.net.InetAddress
import scala.collection.JavaConverters._

object BasicStream {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> conf.getString("kafka.brokers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "example",
      "auto.offset.reset" -> "none"
    ).asJava
    val topics = conf.getString("kafka.topics").split(",").toList.asJava
    val ssc = new StreamingContext(new SparkConf, Seconds(5))
    val stream = DirectKafkaInputDStream[String, String](ssc, kafkaParams, kafkaParams, Map().asJava)
    stream.subscribe(topics)
    stream.seekToBeginning()

    stream.transform { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.mapPartitions { iter =>
        val osr: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        val host = InetAddress.getLocalHost().getHostName()
        val count = iter.size
        Seq(s"${host} ${osr.topic} ${osr.partition} ${count}").toIterator
      }.sortBy(x => x)
    }.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
