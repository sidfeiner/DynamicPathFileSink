package com.sidfeiner.flink.sink.dynamic

import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy

object DynamicPathFileSink {
  def apply(targetDirPath: String, bucketFormat: String, dataTimeBucketFormat: String): StreamingFileSink[DynamicPathRow] = StreamingFileSink
    .forRowFormat(new Path(targetDirPath), new DynamicPathRowEncoder("UTF-8"))
    .withBucketAssigner(new DateTimeAndDynamicPathAssigner(bucketFormat, dataTimeBucketFormat))
    .withRollingPolicy(OnCheckpointRollingPolicy.build())
    .build()
}
