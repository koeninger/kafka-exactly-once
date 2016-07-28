package example

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{KafkaUtils, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import com.typesafe.config.ConfigFactory

/** example of how windowing changes partitioning */
object Windowed {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val ssc = new StreamingContext(new SparkConf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> conf.getString("kafka.brokers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "transactional-example",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "auto.offset.reset" -> "none"
    )

    val topics = conf.getString("kafka.topics").split(",").toSet

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    // reference to the most recently generated input rdd's offset ranges
    var offsetRanges = Array[OffsetRange]()

    stream.transform { rdd =>
      // It's possible to get each input rdd's offset ranges, BUT...
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      println("got offset ranges on the driver:\n" + offsetRanges.mkString("\n"))
      println(s"number of kafka partitions before windowing: ${offsetRanges.size}")
      println(s"number of spark partitions before windowing: ${rdd.partitions.size}")
      rdd
    }.window(Seconds(6), Seconds(2)).foreachRDD { rdd =>
      //... if you then window, you're going to have partitions from multiple input rdds, not just the most recent one
      println(s"number of spark partitions after windowing: ${rdd.partitions.size}")
      rdd.foreachPartition { iter =>
        println("read offset ranges on the executor\n" + offsetRanges.mkString("\n"))
        // notice this partition ID can be higher than the number of partitions in a single input rdd
        println(s"this partition id ${TaskContext.get.partitionId}")
        iter.foreach(println)
      }
      // Moral of the story:
      // If you just care about the most recent rdd's offset ranges, a single reference is fine.
      // If you want to do something with all of the offset ranges in the window,
      // you need to stick them in a data structure, e.g. a bounded queue.

      // But be aware, regardless of whether you use the createStream or createDirectStream api,
      // you will get a fundamentally wrong answer if your job fails and restarts at something other than the highest offset,
      // because the first window after restart will include all messages received while your job was down,
      // not just X seconds worth of messages.

      // In order to really solve this, you'd have to time-index kafka,
      // and override the behavior of the dstream's compute() method to only return messages for the correct time.
      // Or do your own bucketing into a data store based on the time in the message, not system clock at time of reading.

      // Or... don't worry about it :)
      // Restart the stream however you normally would (checkpoint, or save most recent offsets, or auto.offset.reset, whatever)
      // and accept that your first window will be wrong
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
