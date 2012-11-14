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

/**
 * Write scala object to HBase.
 */
object HormWriter {
  val logger = LoggerFactory.getLogger(HormWriter.getClass())

  def write(defaultPath: Array[Byte], obj: HormBaseObject) = {
    val put = new Put(obj.getHBaseId)
    //Convert the value to an Array[Byte] and add it to the put.
    def findType(path: Array[Byte], name: String, value: Any): Unit = {
      logger.debug("find type {} {}", name, value)
      val bytes = value match {
        case v if v == null => null
        case v: Int => Bytes.toBytes(v.asInstanceOf[Int])
        case v: Long => Bytes.toBytes(v.asInstanceOf[Long])
        case v: Float => Bytes.toBytes(v.asInstanceOf[Float])
        case v: String => Bytes.toBytes(v.asInstanceOf[String])
        case v: Boolean => Bytes.toBytes(v.asInstanceOf[Boolean])
        case v: Array[Byte] => v.asInstanceOf[Array[Byte]]
        case v if RawBinder.binders.contains(value.getClass) => RawBinder.binders(value.getClass).write(value)
        case v if v.isInstanceOf[Map[_, _]] => {
          var i = 0
          for ((k, v) <- v.asInstanceOf[Map[_, _]]) {
            logger.debug("getting from map : {} , {}", k.toString, v)
            val p = getNexPath(path, name)
            findType(p, "k" + i, k)
            findType(p, "v" + i, v)
            i = i + 1
          }
          null
        }
        case v: Any => exploreObj(getNexPath(path, name), v); null
        case _ => logger.warn("type is not supported for name {} and path {}", name, Bytes.toString(path)); null
      }
      if (bytes != null) {
        logger.debug("add to path {}:{}", path, name)
        put.add(HormConfig.defaultFamilyName, getNexPath(path, name), bytes)
      }
    }

    //Explore an object using reflection and save all its type one by one, recursivly.
    def exploreObj(family: Array[Byte], obj: Any) = {
      logger.debug("exploring object {} ", obj.getClass)
      for (field <- obj.getClass.getDeclaredFields) {
        field.setAccessible(true)
        val t = field.getType()
        val value = field.get(obj)
        //remove $outer access
        if (!field.getName().startsWith("$")) {
          findType(family, field.getName, value)
        }
      }
    }
    exploreObj(defaultPath, obj)
    put
  }

  //Return the next path (do not add the . if it's the root path)
  private def getNexPath(path: Array[Byte], name: String) = if (path.length == 0) Bytes.toBytes(name) else Bytes.toBytes(Bytes.toString(path) + "." + name)

}