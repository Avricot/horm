package com.avricot.horm.binder.raw

import org.joda.time.DateTime
import org.apache.hadoop.hbase.util.Bytes

/**
 * Jodatime date binder
 */
object DateTimeBinder extends RawBinder[DateTime] {
  def read(param: Array[Byte]) = {
    new DateTime(Bytes.toLong(param))
  }
  def write(obj: Any): Array[Byte] = {
    Bytes.toBytes(obj.asInstanceOf[DateTime].getMillis())
  }
}