# Exactly-once Spark Streaming from Kafka

The upcoming release of [Spark](http://spark.apache.org) 1.3 includes new experimental RDD and DStream implementations for reading data from [Kafka](http://kafka.apache.org).  As the primary author of those features, I'd like to explain their implementation and usage.  You may be interested if you would benefit from:

* more uniform usage of Spark cluster resources when consuming from Kafka
* control of message delivery semantics
* delivery guarantees without reliance on a write-ahead log in HDFS
* access to message metadata

I'll assume you're familiar with the [Spark Streaming docs](http://spark.apache.org/docs/latest/streaming-programming-guide.html) and [Kafka docs](http://kafka.apache.org/documentation.html).  All code examples are in Scala, but there are Java-friendly methods in the API.

## Basic Usage

The new API for both Kafka RDD and DStream is in the spark-streaming-kafka artifact.

SBT dependency:

    libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.3.0"

Maven dependency:

    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-streaming-kafka_2.10</artifactId>
      <version>1.3.0</version>
    </dependency>

To read from Kafka in a Spark Streaming job, use KafkaUtils.createDirectStream:

    import kafka.serializer.StringDecoder
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    import org.apache.spark.streaming.kafka.KafkaUtils

    val ssc = new StreamingContext(new SparkConf, Seconds(60))

    // hostname:port for Kafka brokers, not Zookeeper
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092,anotherhost:9092")

    val topics = Set("sometopic", "anothertopic")

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)


The call to createDirectStream returns a stream of tuples formed from each Kafka message's key and value.  The exposed return type is InputDStream[(K, V)], where K and V in this case are both String.  The private implementation is DirectKafkaInputDStream.  There are other overloads of createDirectStream that allow you to access message metadata, and to specify the exact per-topic-and-partition starting offsets.

To read from Kafka in a non-streaming Spark job, use KafkaUtils.createRDD:

    import kafka.serializer.StringDecoder
    import org.apache.spark.{SparkContext, SparkConf}
    import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}

    val sc = new SparkContext(new SparkConf)

    // hostname:port for Kafka brokers, not Zookeeper
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092,anotherhost:9092")

    val offsetRanges = Array(
      OffsetRange("sometopic", 0, 110, 220),
      OffsetRange("sometopic", 1, 100, 313),
      OffsetRange("anothertopic", 0, 456, 789)
    )

    val rdd = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](
      sc, kafkaParams, offsetRanges)

The call to createRDD returns a single RDD of (key, value) tuples for each Kafka message in the specified batch of offset ranges.  The exposed return type is RDD[(K, V)], the private implementation is KafkaRDD.  There are other overloads of createRDD that allow you to access message metadata, and to specify the current per-topic-and-partition Kafka leaders.

## Implementation

DirectKafkaInputDStream is a stream of batches.  Each batch corresponds to a KafkaRDD.  Each partition of the KafkaRDD corresponds to an OffsetRange.  Most of this implementation is private, but it's still useful to understand.

### OffsetRange

An [OffsetRange](https://github.com/apache/spark/blob/v1.3.0-rc1/external/kafka/src/main/scala/org/apache/spark/streaming/kafka/OffsetRange.scala) represents the lower and upper boundaries for a particular sequence of messages in a given Kafka topic and partition.  The following data structure:

    OffsetRange("visits", 2, 300, 310)

identifies the 10 messages from offset 300 (inclusive) until offset 310 (exclusive) in partition 2 of the "visits" topic.  Note that it does not actually contain the contents of the messages, it's just a way of identifying the range.

Also note that because Kafka ordering is only defined on a per-partition basis, the messages referred to by

    OffsetRange("visits", 3, 300, 310)

may be from a completely different time period; even though the offsets are the same as above, the partition is different.

### KafkaRDD

Recall that an RDD is defined by:

* a method to divide the work into partitions (getPartitions)
* a method to do the work for a given partition (compute)
* a list of parent RDDs.  KafkaRDD is an input, not a transformation, so it has no parents.
* optionally, a partitioner defining how keys are hashed.  KafkaRDD doesn't define one.
* optionally, a list of preferred hosts for a given partition, in order to push computation to where the data is (getPreferredLocations)

The [KafkaRDD constructor](https://github.com/apache/spark/blob/v1.3.0-rc1/external/kafka/src/main/scala/org/apache/spark/streaming/kafka/KafkaRDD.scala#L45) takes an array of OffsetRanges and a map with the current leader host and port for each Kafka topic and partition.  The reason for the separation of leader info is to allow for the KafkaUtils.createRDD convenience constructor that doesn't require you to know the leaders.  In that case, createRDD will do the Kafka API metadata calls necessary to find the current leaders, using the list of hosts specified in metadata.broker.list as the initial points of contact.  That inital lookup will happen once, in the Spark driver process.

The [getPartitions](https://github.com/apache/spark/blob/v1.3.0-rc1/external/kafka/src/main/scala/org/apache/spark/streaming/kafka/KafkaRDD.scala#L57) method of KafkaRDD takes each OffsetRange in the array and turns it into an RDD Partition by adding the leader's host and port info.  The important thing to notice here is there is a 1:1 correspondence between Kafka partition and RDD partition.  This means the degree of Spark parallelism (at least for reading messages) will be directly tied to the degree of Kafka parallelism.

The [getPreferredLocations](https://github.com/apache/spark/blob/v1.3.0-rc1/external/kafka/src/main/scala/org/apache/spark/streaming/kafka/KafkaRDD.scala#L64) method uses the Kafka leader for the given partition as the preferred host.  I don't run my Spark executors on the same hosts as Kafka, so if you do, let me know how this works out for you.

The [compute](https://github.com/apache/spark/blob/v1.3.0-rc1/external/kafka/src/main/scala/org/apache/spark/streaming/kafka/KafkaRDD.scala#L85) method runs in the Spark executor processes.  It uses a Kafka SimpleConsumer to connect to the leader for the given topic and partition, then makes repeated fetch requests to read messages for the specified range of offsets.

Each message is converted using the messageHandler argument to the constructor.  messageHandler is a function from Kafka MessageAndMetadata to a user-defined type, with the default being a tuple of key and value.  In most cases, it's more efficient to access topic and offset metadata on a per-partition basis (see the discussion of HasOffsetRanges below), but if you really need to associate each message with its offset, you can do so.

The key point to notice about compute is that, because offset ranges are defined in advance on the driver, then read directly from Kafka by executors, the messages returned by a particular KafkaRDD are deterministic.  There is no important state maintained on the executors, and no notion of committing read offsets to Zookeeper, as there is with prior solutions that used the Kafka high-level consumer.

Because the compute operation is deterministic, it is in general safe to re-try a task if it fails.  If a Kafka leader is lost, for instance, the compute method will just sleep for the amount of time defined by the **refresh.leader.backoff.ms** Kafka param, then fail the task and let the normal Spark task retry mechanism handle it.  On subsequent attempts after the first, the new leader will be looked up on the executor as part of the compute method.

### DirectKafkaInputDStream

The KafkaRDD returned by KafkaUtils.createRDD is usable in batch jobs if you have existing code to obtain and manage offsets.  In most cases however, you'll probably be using KafkaUtils.createDirectStream, which returns a DirectKafkaInputDStream.  Similar to an RDD, a DStream is defined by:

* a list of parent DStreams.  Again, this is an input DStream, not a transformation, so it has no parents.
* a time interval at which the stream will generate batches.  This stream uses the interval of the streaming context.
* a method to generate an RDD for a given time interval (compute)

The [compute](https://github.com/apache/spark/blob/v1.3.0-rc1/external/kafka/src/main/scala/org/apache/spark/streaming/kafka/DirectKafkaInputDStream.scala#L115) method runs on the driver.  It connects to the leader for each topic and partition, not to read messages, but just to get the latest available offset.  It then defines a KafkaRDD with offset ranges spanning from the ending point of the last batch until the latest leader offsets.

To define the starting point of the very first batch, you can either specify exact offsets per TopicAndPartition, or use the Kafka parameter **auto.offset.reset**, which may be set to "largest" or "smallest" (defaults to "largest").  For rate limiting, you can use the Spark configuration variable **spark.streaming.kafka.maxRatePerPartition** to set the maximum number of messages per partition per batch.

Once the KafkaRDD for a given time interval is defined, it executes exactly as described above for the batch usage case.  Unlike prior Kafka DStream implementations, there is no long-running receiver task that occupies a core per stream regardless of what the message volume is.  For our use cases at [Kixer](http://kixer.com), it's common to have important but low-volume topics in the same job as high-volume topics.  With the direct stream, the low-volume partitions result in smaller tasks that finish quickly and free up that node to process other partitions in the batch.  It's a pretty big win to have uniform cluster usage while still keeping topics logically separate.

A significant difference from the batch use case is that there **is** some important state that varies over time, namely the offset ranges generated at each time interval.  Executor or Kafka leader failure isn't a big deal, as discussed above, but if the driver fails, offset ranges will be lost, unless stored somewhere.  I'll discuss this in more detail under Delivery Semantics below, but you basically have 3 choices:

1.  Don't worry about it if you don't care about lost or duplicated messages, and just restart the stream from the earliest or latest offset
2.  Checkpoint the stream, in which case the offset ranges (not the messages, just the offset range definitions) will be stored in the checkpoint
3.  Store the offset ranges yourself, and provide the correct starting offsets when restarting the stream

Again, no consumer offsets are stored in Zookeeper.  If you want interop with existing Kafka monitoring tools that talk to ZK directly, you'll need to store the offsets into ZK yourself (this doesn't mean it needs to be your system of record for offsets, you can just duplicate them there).

Note that because Kafka is being treated as a durable store of messages, not a transient network source, you don't need to duplicate messages into HDFS for error recovery.  This design does have some implications, however.   The first is that you can't read messages that no longer exist in Kafka, so make sure your retention is adequate.  The second is that you can't read messages that don't exist in Kafka yet. To put it another way, the consumers on the executors aren't polling for new messages, the driver is just periodically checking with the leaders at every batch interval, so there is some inherent latency.

### HasOffsetRanges

One other implementation detail is a public interface, HasOffsetRanges, with a single method returning an array of OffsetRange.  KafkaRDD implements this interface, allowing you to obtain topic and offset information on a per-partition basis.

      val stream = KafkaUtils.createDirectStream(...)
      ...      
      stream.foreachRDD { rdd =>
        // Cast the rdd to an interface that lets us get a collection of offset ranges
        val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        
        rdd.mapPartitionsWithIndex { (i, iter) =>
          // index to get the correct offset range for the rdd partition we're working on
          val osr: OffsetRange = offsets(i)

          // get any needed data from the offset range
          val topic = osr.topic
          val kafkaPartitionId = osr.partition
          val begin = osr.fromOffset
          val end = osr.untilOffset
          ...

The reason for this layer of indirection is because the static type used by DStream methods like foreachRDD and transform is just RDD, not the type of the underlying (and in this case, private) implementation.  Because the DStream returned by createDirectStream generates batches of KafkaRDD, you can safely cast to HasOffsetRanges.

Also note that because of the 1:1 correspondence between offset ranges and rdd partitions, the indexes of the rdd partitions correspond to the indexes into the array returned by offsetRanges.  Similarly, all of the messages in the given Spark partition are from the same Kafka topic. However, this 1:1 correspondence only lasts until the first Spark transformation that incurs a shuffle (e.g. reduceByKey), because shuffles can repartition the data.

        // WON'T DO WHAT YOU WANT
        rdd.mapPartitionsWithIndex { (i, iter) =>
          val osr: OffsetRange = offsets(i)
          iter.map { x =>
            (x.someKey, (osr.topic, x.someValue))
          }
        }.reduceByKey  // this changes partitioning
        .foreachPartition {
           // now the partition contains values from more than one topic
        }

Because of this, if you want to apply a per-partition transformation using offset range information, it's easiest to use normal Scala code to do the work inside a single Spark mapPartitionsWithIndex call.

## Delivery Semantics

First, understand the [Kafka docs on delivery semantics](http://kafka.apache.org/documentation.html#semantics).  If you've already read them, go read them again.  In short: **consumer delivery semantics are up to you**, not Kafka. 

Second, understand that Spark **does not guarantee exactly-once semantics for output actions**.  When the Spark streaming guide talks about exactly-once, it's only referring to a given item in an RDD being included in a calculated value once, in a purely functional sense.  Any side-effecting output operations (i.e. anything you do in foreachRDD to save the result) may be repeated, because any stage of the process might fail and be retried.

Third, understand that Spark **checkpoints may not be recoverable**, for instance in cases where you need to change the application code in order to get the stream restarted.  This situation may improve by 1.4, but be aware that it is an issue. I've been bitten by it before, you may be too.  Any place I mention "checkpoint the stream" as an option, consider the risk involved.  Also note that any windowing transformations are going to rely on checkpointing anyway.

Finally, I'll repeat that any semantics beyond at-most-once require that you have **sufficient log retention in Kafka**.  If you're seeing things like OffsetOutOfRangeException, it's probably because you underprovisioned Kafka storage, not because something's wrong with Spark or Kafka.

Given all that, how do you obtain the equivalent of the semantics you want?

### At-most-once

This could be useful in cases where you're sending results to something that isn't a system of record, you don't want duplicates, and it's not worth the hassle of ensuring that messages don't get lost.  An example might be sending summary statistics over UDP, since it's an unreliable protocol to begin with.

To get at-most-once semantics, do all of the following:

* set **spark.task.maxFailures** to 1, so the job dies as soon as a task fails
* make sure **spark.speculation** is false (the default), so multiple copies of tasks don't get speculatively run
* when the job dies, start the stream back up using the Kafka param **auto.offset.reset** set to "largest", so it will skip to the current end of the log

This will mean you lose messages on restart, but at least they shouldn't get replayed.  Probably.  Test this carefully if it's actually important to you that a message **never** gets repeated, because it's not a common use case, and I'm not providing example code for it.


### At-least-once

You're okay with duplicate messages, but not okay with losing messages.  An example of this might be sending internal email alerts on relatively rare occurrences in the stream.  Getting duplicate critical alerts in a short time frame is much better than not getting them at all.

Basic options here are either

1. Checkpoint the stream *or*
2. restart the job with **auto.offset.reset** set to smallest.  This will replay the whole log from the beginning of your retention, so you'd better have relatively short retention or *really* be ok with duplicate messages.

Checkpointing the stream serves as the basis of the next option, so see the example code for it.

### Exactly-once using idempotent writes

[Idempotent](http://en.wikipedia.org/wiki/Idempotence#Computer_science_meaning) writes make duplicate messages safe, turning at-least-once into the equivalent of exactly-once.  The typical way of doing this is by having a unique key of some kind (either embedded in the message, or using topic/partition/offset as the key), and storing the results according to that key.  Relying on a per-message unique key means this is useful for transforming or filtering individually valuable messages, less so for aggregating multiple messages.

There's a complete sample of this idea at [IdempotentExample.scala](https://github.com/koeninger/kafka-exactly-once/blob/master/src/main/scala/example/IdempotentExample.scala).  It's using postgres for the sake of consistency with the next example, but any storage system that allows for unique keys could be used.

The important points here are that the [schema](https://github.com/koeninger/kafka-exactly-once/blob/master/schema.sql) is set up with a unique key and a rule to allow for no-op duplicate inserts.  For this example, the message body is being used as the unique key, but any appropriate key could be used.

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

In the case of a failure, the above output action can safely be retried.  Checkpointing the stream ensures that offset ranges are saved as they are generated.  Checkpointing is accomplished in the usual way, by defining a function that configures the streaming context (ssc) and sets up the stream, then calling

    ssc.checkpoint(checkpointDir)

before returning the ssc.  See the [streaming guide](http://spark.apache.org/docs/latest/streaming-programming-guide.html#checkpointing) for more details.

### Exactly-once using transactional writes

For data stores that support transactions, saving offsets in the same transaction as the results can keep the two in sync, even in failure situations.  If you're careful about detecting repeated or skipped offset ranges, rolling back the transaction prevents duplicated or lost messages from affecting results.  This gives the equivalent of exactly-once semantics, and is straightforward to use even for aggregations.

[TransactionalExample.scala](https://github.com/koeninger/kafka-exactly-once/blob/master/src/main/scala/example/TransactionalExample.scala) is a complete Spark job implementing this idea.  It's using postgres, but any data store that has transactional semantics could be used.

The first important point is that the stream is started using the last successfully committed offsets as the beginning point.  This allows for failure recovery:

    // begin from the the offsets committed to the database
    val fromOffsets = DB.readOnly { implicit session =>
      sql"select topic, part, off from txn_offsets".
        map { resultSet =>
          TopicAndPartition(resultSet.string(1), resultSet.int(2)) -> resultSet.long(3)
        }.list.apply().toMap
    }

    val stream: InputDStream[Long] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, Long](
      ssc, kafkaParams, fromOffsets,
      // we're just going to count messages, don't care about the contents, so convert each message to a 1
      (mmd: MessageAndMetadata[String, String]) => 1L)

For the very first time the job is run, the table can be pre-loaded with appropriate starting offsets.

The example accesses offset ranges on a per-partition basis, as mentioned in the discussion of HasOffsetRanges above.  The important thing to notice about mapPartitionsWithIndex is that it's a transformation, and there is no equivalent foreachPartitionWithIndex action.  RDD transformations are generally lazy, so unless you add an output action of some kind Spark will never schedule the job to actually do anything.  Calling foreach on the RDD with an empty body is sufficient.  Also notice that some iterator methods, such as map, are lazy.  If you're setting up transient state, like a network or database connection, by the time the map is fully forced the connection may already be closed.  In that case, be sure to instead use methods like foreach, that eagerly consume the iterator.

      rdd.mapPartitionsWithIndex { (i, iter) =>
        // set up some connection

        iter.foreach {
          // use the connection
        }

        // close the connection
        
        Iterator.empty
      }.foreach {
        // Without an action, the job won't get scheduled, so empty foreach to force it
        // This is a little awkward, but there is no foreachPartitionWithIndex method on rdds
        (_: Nothing) => ()
      }

The final thing to notice about the example is that it's important to ensure that saving the results and saving the offsets either both succeed, or both fail.  Storing offsets should fail if the prior committed offset doesn't equal the beginning of the current offset range; this prevents gaps or repeats.  Kafka semantics ensure that there aren't gaps in messages within a range of offsets (if you're especially concerned, you could verify by comparing the size of the offset range to the number of messages).

    // localTx is transactional, if metric update or offset update fails, neither will be committed
    DB.localTx { implicit session =>
      // store metric data
      val metricRows = sql"""
    update txn_data set metric = metric + ${metric}
      where topic = ${osr.topic}
    """.update.apply()
      if (metricRows != 1) {
        throw new Exception("...")
      }

      // store offsets
      val offsetRows = sql"""
    update txn_offsets set off = ${osr.untilOffset}
      where topic = ${osr.topic} and part = ${osr.partition} and off = ${osr.fromOffset}
    """.update.apply()
      if (offsetRows != 1) {
        throw new Exception("...")
      }
    }

The example code is throwing an exception, which will result in a transaction rollback.  Other failure handling strategies may be appropriate, as long as they result in a transaction rollback as well.

## Future Improvements

Although this feature is considered experimental for Spark 1.3, the underlying KafkaRDD design has been in production at [Kixer](http://kixer.com) for months.  It's currently handling billions of messages per day, in batch sizes ranging from 2 seconds to 5 minutes.  That being said, there are known areas for improvement (and probably a few unknown ones as well).

* Connection Pooling.  Currently, Kafka consumer connections are created as needed; pooling should help efficiency.  Hopefully this can be implemented in a way that integrates nicely with ongoing work towards a Kafka producer API in Spark. Edit - I tested out caching consumer connections, it ended up having little to no impact on batch processing time, even with 200ms batches / 100 partitions.
* Kafka metadata API.  The [class for interacting with Kafka](https://github.com/apache/spark/blob/v1.3.0-rc1/external/kafka/src/main/scala/org/apache/spark/streaming/kafka/KafkaCluster.scala) is currently private, meaning you'll need to duplicate some of that work if you want low-level access to Kafka metadata.  This is partly because the Kafka consumer offset API is a moving target right now.  If this code proves to be stable, it would be nice to have a user-facing API for interacting with Kafka metadata.
* Batch generation policies.  Right now, rate-limiting is the only tuning available for how the next batch in the stream is defined.  We have some use cases that involve larger tweaks, such as a fixed time delay.  A flexible way of defining batch generation policies might be useful.

If there are other improvements you can think of, please let me know.
