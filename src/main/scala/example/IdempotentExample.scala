package example

import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import scalikejdbc._

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.rdd.kafka.HasOffsetRanges

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

    println("write some code to lookup your previously existing consumer offsets from zookeeper if you need them")

    println("""note that this will skip to the latest messages after a failure, unless you either checkpoint the stream,
 or save offsets yourself and provide them to the constructor that takes fromOffsets""")

    val stream = KafkaUtils.createNewStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("test"))

    stream.foreachRDD { rdd =>
      rdd.foreachPartition { iter =>
        SetupJdbc()
        iter.foreach { case (key, msg) =>
          DB.autoCommit { implicit session =>
            sql"insert into idem_data(msg) values (${msg})".update.apply
          }
        }
      }
      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      println("write some code to store zookeeper offsets if you want compatibility with existing monitoring.")
      offsets.foreach { o =>
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
