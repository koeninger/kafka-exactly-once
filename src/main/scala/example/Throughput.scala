package example

import kafka.serializer.DefaultDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import com.typesafe.config.ConfigFactory

object Throughput {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load

    val ssc = new StreamingContext(new SparkConf, Milliseconds(conf.getLong("batchDurationMs")))

    val kafkaParams = Map("metadata.broker.list" -> conf.getString("kafka.brokers"))

    val topics = conf.getString("kafka.topics").split(",").toSet

    val stream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
      ssc, kafkaParams, topics)

    stream.foreachRDD { rdd =>
      println(rdd.map(_._2.size).fold(0)(_+_))
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
