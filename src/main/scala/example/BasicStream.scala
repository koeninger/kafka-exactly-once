package example

object BasicStream {
    import kafka.serializer.StringDecoder
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    import org.apache.spark.streaming.kafka.KafkaUtils

    val ssc = new StreamingContext(new SparkConf, Seconds(60))

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092,anotherhost:9092")

    val topics = Set("sometopic", "anothertopic")

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

}
