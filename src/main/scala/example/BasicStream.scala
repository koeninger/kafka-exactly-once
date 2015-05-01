package example

object BasicStream {
  def main(args: Array[String]) {
    import kafka.serializer.StringDecoder
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    import org.apache.spark.streaming.kafka.KafkaUtils

    val ssc = new StreamingContext(new SparkConf, Seconds(5))

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092,localhost:9093,localhost:9094")

    val topics = args.toSet

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    stream.print

    ssc.awaitTermination()
  }
}
