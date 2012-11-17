package com.avricot.horm.binder.complex

import org.apache.hadoop.hbase.client.Put
import com.avricot.horm.binder.raw.ComplexBinder
import org.slf4j.LoggerFactory
import com.avricot.horm.writer.HormWriter

object MapBinder extends ComplexBinder[scala.collection.mutable.Map[_, _]] {
  val logger = LoggerFactory.getLogger(MapBinder.getClass())

  override def read(param: Array[Byte]) = {
    null
  }
  override def write(path: Array[Byte], fieldName: String, obj: Any, put: Put) = {
    var i = 0
    val p = HormWriter.getNexPath(path, fieldName)
    for ((k, v) <- obj.asInstanceOf[scala.collection.Map[_, _]]) {
      logger.debug("getting from map : {} , {}", k.toString, v)
      HormWriter.findType(p, "k" + i, k, put)
      HormWriter.findType(p, "v" + i, v, put)
      i = i + 1
    }
  }

}