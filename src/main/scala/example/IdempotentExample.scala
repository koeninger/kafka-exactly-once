package example

import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import scalikejdbc._

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.kafka.{KafkaCluster, KafkaRDD}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.DeterministicKafkaInputDStream

/** exactly-once semantics from kafka, by storing data idempotently so that replay is safe,
  and only storing offsets after data storage succeeds */
object IdempotentExample {
  val schema = """
create table idem_data(
  msg character varying(255),
  primary key (msg)
);

-- postgres isnt the best for idempotent storage, this is for example purposes only
create or replace rule idem_data_ignore_duplicate_inserts as
  on insert to idem_data
  where (exists (select 1 from idem_data where idem_data.msg = new.msg))
  do instead nothing
;
"""

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext(new SparkConf, Seconds(60))
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092,localhost:9093,localhost:9094")

    // could also use kc.getPartitions instead of hardcoding
    val topicPartitions = Set(
      TopicAndPartition("test", 0),
      TopicAndPartition("test", 1)
    )

    // don't need to save offsets in a transaction, so example of using zookeeper / kafka offsets
    val kc = new KafkaCluster(kafkaParams)
    val groupId = "testConsumer"
    val fromOffsets = kc.getConsumerOffsets(groupId, topicPartitions).right.toOption.
      orElse(kc.getEarliestLeaderOffsets(topicPartitions).right.toOption).
      getOrElse(throw new Exception("couldn't get consumer offsets"))

    val stream = new DeterministicKafkaInputDStream[String, String, StringDecoder, StringDecoder, String](
      ssc, kafkaParams, fromOffsets, messageAndMetadata => messageAndMetadata.message)

    stream.foreachRDD { rdd =>
      rdd.foreachPartition { iter =>
        SetupJdbc()
        iter.foreach { msg =>
          DB.autoCommit { implicit session =>
            sql"insert into idem_data(msg) values (${msg})".update.apply
          }
        }
      }
      val kafkaRDD = rdd.asInstanceOf[KafkaRDD[String, String, StringDecoder, StringDecoder, String]]
      kc.setConsumerOffsets(groupId, kafkaRDD.untilOffsets).fold(
        err => throw new Exception("couldn't set consumer offsets"),
        ok => ()
      )
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
