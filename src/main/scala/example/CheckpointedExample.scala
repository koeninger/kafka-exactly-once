package example

import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import scalikejdbc._

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.kafka.{KafkaCluster, KafkaRDDPartition}
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.DeterministicKafkaInputDStream

object CheckpointedExample {
  val schema = """
create table cp_data(
  topic character varying(255),
  part integer,
  off bigint,
  window_min bigint,
  tstamp bigint
);

insert into cp_data(topic, part, off, window_min, tstamp) values
-- or whatever your initial offsets are, if non-0
  ('test', 0, 0, 0, 0),
  ('test', 1, 0, 0, 0)
;
"""

  def main(args: Array[String]): Unit = {
    SetupJdbc()
    val checkpointDirectory = "/var/tmp/cp"
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, setupSsc(checkpointDirectory) _)
    println("setup streaming context at " + System.currentTimeMillis)
    ssc.start()
    ssc.awaitTermination()
  }

  def setupSsc(checkpointDirectory: String)(): StreamingContext = {
    val ssc = new StreamingContext(new SparkConf, Seconds(10))
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092,localhost:9093,localhost:9094")

    val fromOffsets = DB.readOnly { implicit session =>
      sql"select topic, part, max(off) from cp_data group by topic, part".
        map { resultSet =>
          TopicAndPartition(resultSet.string(1), resultSet.int(2)) -> resultSet.long(3)
        }.list.apply().toMap
    }

    val stream = new DeterministicKafkaInputDStream[String, String, StringDecoder, StringDecoder, Long](
      ssc, kafkaParams, fromOffsets, messageAndMetadata => messageAndMetadata.message.toLong)

    stream.transform { rdd =>
      rdd.mapPartitionsWithIndex { (i, iter) =>
        val rp = rdd.partitions(i).asInstanceOf[KafkaRDDPartition]
        iter.map { msg =>
          (rp.topic, rp.partition) -> (rp.untilOffset, msg)
        }
      }
    }.reduceByKeyAndWindow({ (a: (Long, Long), b: (Long, Long)) =>
      (Math.max(a._1, b._1), Math.min(a._2, b._2))
    }, Seconds(60), Seconds(10)).foreachRDD { rdd =>
      rdd.foreach { kv =>
        SetupJdbc()
        DB.localTx { implicit session =>
          val ((topic, partition), (untilOffset, minMsg)) = kv
          val ts = System.currentTimeMillis
          sql"""
insert into cp_data(topic, part, off, window_min, tstamp) 
values (${topic}, ${partition}, ${untilOffset}, ${minMsg}, ${ts})
""".update.apply
        }
      }
    }
    ssc.checkpoint(checkpointDirectory)
    ssc
  }
}
