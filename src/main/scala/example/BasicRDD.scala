package example

object BasicRDD {
    import kafka.serializer.StringDecoder
    import org.apache.spark.{SparkContext, SparkConf}
    import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}

    val sc = new SparkContext(new SparkConf())

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092,anotherhost:9092")

    val offsetRanges = Array(
      OffsetRange("sometopic", 0, 110, 220),
      OffsetRange("sometopic", 1, 100, 313),
      OffsetRange("anothertopic", 0, 456, 789)
    )

    val rdd = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](
      sc, kafkaParams, offsetRanges)

}
