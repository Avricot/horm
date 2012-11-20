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
import scala.collection.Seq
import java.lang.reflect.Field
import com.avricot.horm.HormMap

/**
 * Write scala object to HBase.
 */
object HormWriter {
  val logger = LoggerFactory.getLogger(HormWriter.getClass())

  def write(defaultPath: Array[Byte], obj: HormBaseObject) = {
    val put = new Put(obj.getHBaseId)
    findType(defaultPath, "", null, obj, put)
    put
  }

  //Convert the value to an Array[Byte] and add it to the put.
  def findType(path: Array[Byte], name: String, field: Field, value: Any, put: Put): Unit = {
    if (value == null) {
      return
    }
    logger.debug("find type {} {}", name, value)
    logger.debug("path {}", Bytes.toString(path))
    value match {
      case v if v == null => null
      case v if RawBinder.binders.contains(value.getClass) => {
        val bytes = RawBinder.binders(value.getClass).write(value)
        if (bytes != null) {
          logger.debug("add to path {}:{}", path, name)
          put.add(HormConfig.defaultFamilyName, getNexPath(path, name), bytes)
        }
      }
      case v if ComplexBinder.binders.contains(value.getClass) => ComplexBinder.binders(value.getClass).write(getNexPath(path, name), name, field, v, put)
      case v if v.isInstanceOf[Map[_, _]] => {
        val hormMap = field.getAnnotation(classOf[HormMap])
        val typeKey = hormMap match {
          case null => classOf[String]
          case _ => hormMap.key()
        }
        var i = 0
        val p = getNexPath(path, name)
        for ((k, v) <- v.asInstanceOf[Map[_, _]]) {
          logger.debug("getting from map : {} , {}", k.toString, v)
          //String as key
          if (typeKey == classOf[String]) {
            findType(p, k.asInstanceOf[String], null, v, put)
          } else {
            findType(p, "k" + i, null, k, put)
            findType(p, "v" + i, null, v, put)
            i = i + 1
          }
        }
      }
      case v if v.isInstanceOf[Seq[_]] || v.isInstanceOf[Set[_]] => {
        var i = 0
        for (e <- v.asInstanceOf[Seq[_]]) {
          logger.debug("getting from seq : {} ", v)
          val p = getNexPath(path, name)
          findType(p, i.toString, null, e, put)
          i = i + 1
        }
      } case v: Any => ObjectBinder.write(getNexPath(path, name), name, field, v, put)
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