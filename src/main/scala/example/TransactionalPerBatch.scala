package example

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.TopicPartition

import scalikejdbc._
import com.typesafe.config.ConfigFactory

import org.apache.spark.{SparkContext, SparkConf, TaskContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.{ KafkaUtils, HasOffsetRanges, OffsetRange }
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Assign
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import scala.collection.JavaConverters._

/** exactly-once semantics from kafka, by storing offsets in the same transaction as the results
  Offsets and results will be stored per-batch, on the driver
  */
object TransactionalPerBatch {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> conf.getString("kafka.brokers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "transactional-example",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "auto.offset.reset" -> "none"
    )
    val jdbcDriver = conf.getString("jdbc.driver")
    val jdbcUrl = conf.getString("jdbc.url")
    val jdbcUser = conf.getString("jdbc.user")
    val jdbcPassword = conf.getString("jdbc.password")

    val ssc = setupSsc(kafkaParams, jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword)()
    ssc.start()
    ssc.awaitTermination()

  }

  def setupSsc(
    kafkaParams: Map[String, Object],
    jdbcDriver: String,
    jdbcUrl: String,
    jdbcUser: String,
    jdbcPassword: String
  )(): StreamingContext = {
    val ssc = new StreamingContext(new SparkConf, Seconds(60))

    SetupJdbc(jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword)

    // begin from the the offsets committed to the database
    val fromOffsets = DB.readOnly { implicit session =>
      sql"select topic, part, off from txn_offsets".
        map { resultSet =>
          new TopicPartition(resultSet.string(1), resultSet.int(2)) -> resultSet.long(3)
        }.list.apply().toMap
    }

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
    ).map { record =>
      // we're just going to count messages per topic, don't care about the contents, so convert each message to (topic, 1)
      (record.topic, 1L)
    }

    stream.foreachRDD { rdd =>
      // Note this block is running on the driver

      // Cast the rdd to an interface that lets us get an array of OffsetRange
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // simplest possible "metric", namely a count of messages per topic
      // Notice the aggregation is done using spark methods, and results collected back to driver
      val results = rdd.reduceByKey {
        // This is the only block of code running on the executors.
        // reduceByKey did a shuffle, but that's fine, we're not relying on anything special about partitioning here
        _+_
      }.collect

      // Back to running on the driver

      // localTx is transactional, if metric update or offset update fails, neither will be committed
      DB.localTx { implicit session =>
        // store metric results
        results.foreach { pair =>
          val (topic, metric) = pair
          val metricRows = sql"""
update txn_data set metric = metric + ${metric}
  where topic = ${topic}
""".update.apply()
          if (metricRows != 1) {
            throw new Exception(s"""
Got $metricRows rows affected instead of 1 when attempting to update metrics for $topic
""")
          }
        }

        // store offsets
        offsetRanges.foreach { osr =>
          val offsetRows = sql"""
update txn_offsets set off = ${osr.untilOffset}
  where topic = ${osr.topic} and part = ${osr.partition} and off = ${osr.fromOffset}
""".update.apply()
          if (offsetRows != 1) {
            throw new Exception(s"""
Got $offsetRows rows affected instead of 1 when attempting to update offsets for
 ${osr.topic} ${osr.partition} ${osr.fromOffset} -> ${osr.untilOffset}
Was a partition repeated after a worker failure?
""")
          }
        }
      }
    }
    ssc
  }
}
