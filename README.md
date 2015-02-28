kafka-exactly-once
==================
see https://issues.apache.org/jira/browse/SPARK-4964

If you want to try running this,

schema.sql contains postgres schemas for the tables used

src/main/resources/application.conf contains jdbc and kafka config info

The examples are indifferent to the exact kafka topic or message format used,
although IdempotentExample assumes each message body is unique.