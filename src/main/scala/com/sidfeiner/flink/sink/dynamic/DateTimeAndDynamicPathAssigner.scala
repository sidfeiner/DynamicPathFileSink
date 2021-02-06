package com.sidfeiner.flink.sink.dynamic

import java.util.TimeZone

import com.sidfeiner.utils.DateTimeUtils
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer

/**
 * @param pathFormat Format of path that must be filled with the dynamic values
 * @param timeZone   TimeZone for date time utils
 */
class DateTimeAndDynamicPathAssigner(pathFormat: String, timeZone: TimeZone = TimeZone.getDefault)
  extends BucketAssigner[DynamicPathRow, String] {

  val dateTimeUtils = new DateTimeUtils(timeZone)
  val defaultFillers: Map[String, () => Any] = Map(
    "YEAR" -> dateTimeUtils.getCurrentYear,
    "MONTH" -> dateTimeUtils.getCurrentMonth,
    "DAY" -> dateTimeUtils.getCurrentDayOfMonth,
    "HOUR" -> dateTimeUtils.getCurrentHour,
    "MINUTE" -> dateTimeUtils.getCurrentMinute,
    "SECOND" -> dateTimeUtils.getCurrentSecond
  ).asInstanceOf[Map[String, () => Any]].filterKeys(key => pathFormat.contains(s"[$key]"))

  def fillTarget(fillers: Map[String, String]): String = {
    val defaultFinalFillers = defaultFillers.mapValues(f => f().toString)
    (fillers ++ defaultFinalFillers).foldRight(pathFormat) { case ((placeholder, value), path) =>
      path.replace(s"[$placeholder]", value)
    }
  }

  override def getBucketId(element: DynamicPathRow, context: BucketAssigner.Context): String = {
    fillTarget(element.fileNameFillers)
  }

  override def getSerializer: SimpleVersionedSerializer[String] = SimpleVersionedStringSerializer.INSTANCE
}
