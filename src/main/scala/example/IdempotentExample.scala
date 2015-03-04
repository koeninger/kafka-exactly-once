package example

import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import scalikejdbc._
import com.typesafe.config.ConfigFactory

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.{KafkaUtils, HasOffsetRanges}

/** exactly-once semantics from kafka, by storing data idempotently so that replay is safe */
object IdempotentExample {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val topics = conf.getString("kafka.topics").split(",").toSet
    val kafkaParams = Map(
      "metadata.broker.list" -> conf.getString("kafka.brokers"),
      // start from the smallest available offset, ie the beginning of the kafka log
      "auto.offset.reset" -> "smallest"
    )
    val jdbcDriver = conf.getString("jdbc.driver")
    val jdbcUrl = conf.getString("jdbc.url")
    val jdbcUser = conf.getString("jdbc.user")
    val jdbcPassword = conf.getString("jdbc.password")

    // while the job doesn't strictly need checkpointing,
    // we'll checkpoint to avoid replaying the whole kafka log in case of failure
    val checkpointDir = conf.getString("checkpointDir")

    val ssc = StreamingContext.getOrCreate(
      checkpointDir,
      setupSsc(topics, kafkaParams, jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword, checkpointDir) _
    )
    ssc.start()
    ssc.awaitTermination()
  }

  def setupSsc(
    topics: Set[String],
    kafkaParams: Map[String, String],
    jdbcDriver: String,
    jdbcUrl: String,
    jdbcUser: String,
    jdbcPassword: String,
    checkpointDir: String
  )(): StreamingContext = {
    val ssc = new StreamingContext(new SparkConf, Seconds(60))

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    stream.foreachRDD { rdd =>
      rdd.foreachPartition { iter =>
        // make sure connection pool is set up on the executor before writing
        SetupJdbc(jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword)

        iter.foreach { case (key, msg) =>
          DB.autoCommit { implicit session =>
            // the unique key for idempotency is just the text of the message itself, for example purposes
            sql"insert into idem_data(msg) values (${msg})".update.apply
          }
        }
      }
    }
    // the offset ranges for the stream will be stored in the checkpoint
    ssc.checkpoint(checkpointDir)
    ssc
  }
}
