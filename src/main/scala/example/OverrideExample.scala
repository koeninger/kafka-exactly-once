package example

import scala.reflect.ClassTag

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder

import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd.kafka.KafkaRDD
import org.apache.spark.streaming.kafka.DeterministicKafkaInputDStream

// example of overriding DeterministicKafkaInputDStream if you need different logic for how batches are computed
class OverrideKafkaInputDStream[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag,
  R: ClassTag](
    @transient ssc_ : StreamingContext,
    override val kafkaParams: Map[String, String],
    override val fromOffsets: Map[TopicAndPartition, Long],
    messageHandler: MessageAndMetadata[K, V] => R,
    maxRetries: Int = 1
) extends DeterministicKafkaInputDStream[K, V, U, T, R](
  ssc_, kafkaParams, fromOffsets, messageHandler, maxRetries
) {

  override def compute(validTime: Time): Option[KafkaRDD[K, V, U, T, R]] = {
    // some arbitrary computation to get the offsets for the next batch...
    val untilOffsets = latestLeaderOffsets(maxRetries).map { case (tp, lo) =>
        tp -> lo.copy(offset = lo.offset - 23)
    }
    val rdd = KafkaRDD[K, V, U, T, R](
      context.sparkContext, kafkaParams, currentOffsets, untilOffsets, messageHandler)

    currentOffsets = untilOffsets.map(kv => kv._1 -> kv._2.offset)
    Some(rdd)
  }
}
