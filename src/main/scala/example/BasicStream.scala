package example

import kafka.serializer.StringDecoder
import org.apache.spark.{ SparkConf, TaskContext }
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{ KafkaUtils, OffsetRange, HasOffsetRanges }
import com.typesafe.config.ConfigFactory
import java.net.InetAddress

object BasicStream {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val kafkaParams = Map(
      "metadata.broker.list" -> conf.getString("kafka.brokers")
    )
    val topics = conf.getString("kafka.topics").split(",").toSet
    val ssc = new StreamingContext(new SparkConf, Seconds(5))
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

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
