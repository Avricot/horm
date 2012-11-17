package com.avricot.horm.binder.complex

import org.apache.hadoop.hbase.client.Put
import com.avricot.horm.binder.raw.ComplexBinder
import org.slf4j.LoggerFactory
import com.avricot.horm.writer.HormWriter

object ObjectBinder extends ComplexBinder[Map[_, _]] {
  val logger = LoggerFactory.getLogger(ObjectBinder.getClass())

  override def read(param: Array[Byte]) = {
    null
  }
  override def write(family: Array[Byte], fieldName: String, obj: Any, put: Put) = {
    logger.debug("exploring object {} ", obj.getClass)
    for (field <- obj.getClass.getDeclaredFields) {
      field.setAccessible(true)
      val t = field.getType()
      val value = field.get(obj)
      //remove $outer access
      if (!field.getName().startsWith("$")) {
        HormWriter.findType(family, field.getName, value, put)
      }
    }
  }

}