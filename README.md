kafka-exactly-once
==================
Usage examples for the Kafka createDirectStream / createRDD api that I contributed to Spark (available since Spark 1.3)

Master corresponds to Spark 2.0 / Kafka 0.10

If you're looking for earlier versions, see the [Spark 1.6 branch](https://github.com/koeninger/kafka-exactly-once/tree/spark-1.6.0)

For more detail, see the [presentation](https://www.youtube.com/watch?v=fXnNEq1v3VA) or the [blog post](https://github.com/koeninger/kafka-exactly-once/blob/master/blogpost.md) or the [slides](http://koeninger.github.io/kafka-exactly-once/) or the [jira ticket](https://issues.apache.org/jira/browse/SPARK-4964)

If you want to try running this,

schema.sql contains postgres schemas for the tables used

src/main/resources/application.conf contains jdbc and kafka config info

The examples are indifferent to the exact kafka topic or message format used,
although IdempotentExample assumes each message body is unique.
