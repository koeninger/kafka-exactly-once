# Exactly-once Spark Streaming from Kafka

The upcoming release of [Spark](http://spark.apache.org) 1.3 includes new experimental RDD and DStream implementations for reading data from [Kafka](http://kafka.apache.org).  As the primary author of those features, I'd like to explain their implementation and usage.  You may be interested if you would benefit from:

* more uniform usage of Spark cluster resources when consuming from Kafka
* control of message delivery semantics
* delivery guarantees without reliance on a write-ahead log in HDFS
* access to message metadata

I'll assume you're familiar with the [Spark Streaming docs](http://spark.apache.org/docs/latest/streaming-programming-guide.html) and [Kafka docs](http://kafka.apache.org/documentation.html).  All code examples are in Scala, but there are Java-friendly methods in the API.

## Basic Usage

The new api for both Kafka RDD and DStream is in the spark-streaming-kafka artifact.  If version 1.3.0 of Spark is not yet released by the time you're reading this, you'll need to build it locally.

    libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.3.0"

To read from Kafka in a Spark Streaming job, use KafkaUtils.createDirectStream:

    import kafka.serializer.StringDecoder
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    import org.apache.spark.streaming.kafka.KafkaUtils

    val ssc = new StreamingContext(new SparkConf, Seconds(60))

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092,anotherhost:9092")

    val topics = Set("sometopic", "anothertopic")

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)


The call to createDirectStream returns a stream of tuples formed from each Kafka message's key and value.  The exposed return type is InputDStream[(K, V)], where K and V in this case are both String.  The private implementation is DirectKafkaInputDStream.  There are other overloads of createDirectStream that allow you to access message metadata, and to specify the exact per-topic-and-partition offsets to begin at.

To read from Kafka in a non-streaming Spark job, use KafkaUtils.createRDD:

    import kafka.serializer.StringDecoder
    import org.apache.spark.{SparkContext, SparkConf}
    import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}

    val sc = new SparkContext(new SparkConf)

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092,anotherhost:9092")

    val offsetRanges = Array(
      OffsetRange("sometopic", 0, 110, 220),
      OffsetRange("sometopic", 1, 100, 313),
      OffsetRange("anothertopic", 0, 456, 789)
    )

    val rdd = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](
      sc, kafkaParams, offsetRanges)

The call to createRDD returns a single RDD of (key, value) tuples for each Kafka message in the specified batch of offset ranges.  The exposed return type is RDD[(K, V)], the private implementation is KafkaRDD.  There are other overloads of createRDD that allow you to access message metadata, and to specify the current per-topic-and-partition Kafka leaders (to avoid a duplicate lookup, if you already have them).

## Implementation

DirectKafkaInputDStream is a stream of batches.  Each batch corresponds to a KafkaRDD.  Each partition of the KafkaRDD corresponds to an OffsetRange.

### OffsetRange

An [OffsetRange](https://github.com/apache/spark/blob/v1.3.0-rc1/external/kafka/src/main/scala/org/apache/spark/streaming/kafka/OffsetRange.scala) represents the lower and upper boundaries for a particular sequence of messages in a given Kafka topic and partition.  The following data structure:

    OffsetRange("visits", 2, 300, 310)

identifies the 10 messages from offset 300 (inclusive) until offset 310 (exclusive) in partition 2 of the "visits" topic.  Note that it does not actually contain the contents of the messages, it's just a way of identifying the range.


### KafkaRDD

Recall that an RDD is defined by:

* a method to divide the work into partitions (getPartitions)
* a method to do the work for a given partition (compute)
* a list of parent RDDs.  KafkaRDD is an input, not a transformation, so it has no parents.
* optionally, a partitioner defining how keys are hashed.  KafkaRDD doesn't define one.
* optionally, a list of preferred hosts for a given partition, in order to push computation to where the data is (getPreferredLocations)

