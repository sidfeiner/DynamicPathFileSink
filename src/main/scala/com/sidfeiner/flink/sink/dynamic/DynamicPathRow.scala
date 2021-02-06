package com.sidfeiner.flink.sink.dynamic

import java.io.OutputStream

import org.apache.flink.api.common.serialization.Encoder

/**
 * @param fileNameFillers Map of keys and values for dynamic parts in path
 * @param row             Data to be written in the file
 */
case class DynamicPathRow(fileNameFillers: Map[String, String], row: String)

class DynamicPathRowEncoder(encoding: String) extends Encoder[DynamicPathRow] {
  override def encode(element: DynamicPathRow, stream: OutputStream): Unit = stream.write(element.row.getBytes(encoding))
}