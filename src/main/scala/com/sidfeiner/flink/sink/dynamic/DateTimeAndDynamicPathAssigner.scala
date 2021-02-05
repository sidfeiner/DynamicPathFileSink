package com.sidfeiner.flink.sink.dynamic

import java.nio.file.Paths
import java.time.ZoneId

import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.{DateTimeBucketAssigner, SimpleVersionedStringSerializer}

class DateTimeAndDynamicPathAssigner(pathFormat: String, dateTimeBucketFormat: String) extends BucketAssigner[DynamicPathRow, String] {

  private val dateTimeAssigner = new DateTimeBucketAssigner[String](dateTimeBucketFormat, ZoneId.of("UTC"))

  def fillTarget(fillers: Map[String, String]): String = {
    fillers.foldRight(pathFormat) { case ((placeholder, value), path) =>
      path.replace(s"[$placeholder]", value)
    }
  }

  override def getBucketId(element: DynamicPathRow, context: BucketAssigner.Context): String = {
    val formattedPart = fillTarget(element.fileNameFillers)
    val dateTimeBucket = dateTimeAssigner.getBucketId(element.row, context)
    Paths.get(formattedPart, dateTimeBucket).toString
  }

  override def getSerializer: SimpleVersionedSerializer[String] = SimpleVersionedStringSerializer.INSTANCE
}
