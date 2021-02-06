package com.sidfeiner.flink.sink.dynamic

import java.util.TimeZone

import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy

object DynamicPathFileSink {
  /**
   * @param targetDirPath Base directory where all data will be written
   * @param bucketFormat  Format of directory that will be created under `targetDirPath`.
   *                      Dynamic values must be: [KEY]. For example: `[COUNTRY]/[YEAR]/`
   *                      Supports the following default placeholders: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
   * @param timeZone      TimeZone that will be used if any of the time placeholders are used
   * @return StreamingFileSink that receives a DynamicPathrow
   */
  def apply(targetDirPath: String, bucketFormat: String, timeZone: TimeZone = TimeZone.getDefault): StreamingFileSink[DynamicPathRow] = StreamingFileSink
    .forRowFormat(new Path(targetDirPath), new DynamicPathRowEncoder("UTF-8"))
    .withBucketAssigner(new DateTimeAndDynamicPathAssigner(bucketFormat, timeZone))
    .withRollingPolicy(OnCheckpointRollingPolicy.build())
    .build()
}
