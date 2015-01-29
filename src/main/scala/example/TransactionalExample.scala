package example

import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import scalikejdbc._

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.rdd.kafka.OffsetRange
import org.apache.spark.streaming.kafka.KafkaUtils

/** exactly-once semantics from kafka, by storing offsets in the same transaction as the data */
object TransactionalExample {
  val schema = """
create table txn_data(
  msg character varying(255)
);

create table txn_offsets(
  topic character varying(255),
  part integer,
  off bigint,
  unique (topic, part)
);

insert into txn_offsets(topic, part, off) values
-- or whatever your initial offsets are, if non-0
  ('test', 0, 0),
  ('test', 1, 0)
;
"""

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext(new SparkConf, Seconds(60))
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092,localhost:9093,localhost:9094")

    SetupJdbc()
    val fromOffsets = DB.readOnly { implicit session =>
      sql"select topic, part, off from txn_offsets".
        map { resultSet =>
          TopicAndPartition(resultSet.string(1), resultSet.int(2)) -> resultSet.long(3)
        }.list.apply().toMap
    }

    val retries = 2

    val stream = KafkaUtils.createNewStream[String, String, StringDecoder, StringDecoder, String](
      ssc, kafkaParams, fromOffsets, messageAndMetadata => messageAndMetadata.message, retries)

    stream.foreachRDD { rdd =>
      rdd.foreachPartitionWithIndex { (i, iter) =>
        SetupJdbc()
        val rp = rdd.partitions(i).asInstanceOf[OffsetRange]
        DB.localTx { implicit session =>
          iter.foreach { msg =>
            sql"insert into txn_data(msg) values (${msg})".update.apply
          }
          val updated = sql"""
update txn_offsets set off = ${rp.untilOffset}
  where topic = ${rp.topic} and part = ${rp.partition} and off = ${rp.fromOffset}
""".update.apply()
          if (updated != 1) {
            throw new Exception(s"""
Got $updated rows affected instead of 1 when attempting to update offsets for
 ${rp.topic} ${rp.partition} ${rp.fromOffset} -> ${rp.untilOffset}
Was a partition repeated after a worker failure?
""")
          }
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
