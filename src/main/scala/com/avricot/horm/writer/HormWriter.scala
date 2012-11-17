package com.avricot.horm.writer

import org.slf4j.LoggerFactory
import org.joda.time.DateTime
import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.format.ISODateTimeFormat
import org.apache.hadoop.hbase.client.Put
import scala.collection.Map
import com.avricot.horm.HormConfig
import com.avricot.horm.HormBaseObject
import com.avricot.horm.binder.raw.RawBinder
import com.avricot.horm.binder.raw.ComplexBinder
import com.avricot.horm.binder.complex.ObjectBinder

/**
 * Write scala object to HBase.
 */
object HormWriter {
  val logger = LoggerFactory.getLogger(HormWriter.getClass())

  def write(defaultPath: Array[Byte], obj: HormBaseObject) = {
    val put = new Put(obj.getHBaseId)
    findType(defaultPath, "", obj, put)
    put
  }

  //Convert the value to an Array[Byte] and add it to the put.
  def findType(path: Array[Byte], name: String, value: Any, put: Put): Unit = {
    println("find type " + name)
    if (value == null) {
      return
    }
    println("find type " + name + ", " + value.getClass)
    logger.debug("find type {} {}", name, value)
    val bytes = value match {
      case v if v == null => null
      case v if RawBinder.binders.contains(value.getClass) => {
        val bytes = RawBinder.binders(value.getClass).write(value)
        if (bytes != null) {
          logger.debug("add to path {}:{}", path, name)
          put.add(HormConfig.defaultFamilyName, getNexPath(path, name), bytes)
        }
      }
      case v if ComplexBinder.binders.contains(value.getClass) => ComplexBinder.binders(value.getClass).write(getNexPath(path, name), name, v, put)
      case v: Any => ObjectBinder.write(getNexPath(path, name), name, v, put)
      case _ => logger.warn("type is not supported for name {} and path {}", name, Bytes.toString(path)); null
    }
  }

  //Return the next path (do not add the . if it's the root path)
  def getNexPath(path: Array[Byte], name: String) = {
    if (path.length == 0) {
      Bytes.toBytes(name)
    } else {
      Bytes.toBytes(Bytes.toString(path) + "." + name)
    }
  }

}