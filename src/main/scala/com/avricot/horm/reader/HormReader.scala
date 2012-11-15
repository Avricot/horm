package com.avricot.horm.reader

import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer
import org.joda.time.DateTime
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import java.lang.reflect.Type
import java.lang.reflect.ParameterizedType
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTime
import com.avricot.horm.HormMap
import com.avricot.horm.HormMap
import java.lang.reflect.Field
import com.avricot.horm.binder.raw.RawBinder
import scala.collection.mutable.ListBuffer

/**
 * Read scala object from hbase.
 */
object HormReader {
  val logger = LoggerFactory.getLogger(HormReader.getClass())

  val BY = classOf[Byte]
  val I = classOf[Int]
  val L = classOf[Long]
  val F = classOf[Float]
  val S = classOf[String]
  val B = classOf[Boolean]
  val D = classOf[DateTime]
  val A = classOf[Array[Byte]]
  val MM = classOf[scala.collection.mutable.Map[_, _]]
  val IM = classOf[scala.collection.immutable.Map[_, _]]
  val SM = classOf[scala.collection.Map[_, _]]

  /**
   * Build an object from a result
   */
  def read(result: Result, klass: Class[_]): Any = {
    if (result.isEmpty()) {
      return None
    }
    //Return the default value of AnyVals.
    def getDefaultValue(klass: Class[_]): Any = {
      klass match {
        case BY => 0
        case I => 0
        case L => 0L
        case F => 0.0
        case B => false
        case _ => null
      }
    }
    //Return the value from the given Array[Byte]
    def getValue(klass: Class[_], fieldValue: Array[Byte]) = {
      klass match {
        case BY => fieldValue(0)
        case I => Bytes.toInt(fieldValue)
        case L => Bytes.toLong(fieldValue)
        case F => Bytes.toFloat(fieldValue)
        case S => Bytes.toString(fieldValue)
        case B => Bytes.toBoolean(fieldValue)
        case A => fieldValue
        case v if RawBinder.binders.contains(klass) => RawBinder.binders(klass).read(fieldValue)
        case _ => logger.warn("Horm can't map this class ! {}", klass); null
      }
    }

    //Init and sort the data by column family.
    val objArgs = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Array[Byte]]]("" -> scala.collection.mutable.Map[String, Array[Byte]]());
    for (kv <- result.raw()) {
      val splitKV = kv.split()
      val fullName = Bytes.toString(splitKV.getQualifier())
      val containsDot = fullName.contains(".")
      val (path, fieldName) = if (!containsDot) ("", fullName) else (fullName.substring(0, fullName.lastIndexOf(".")), fullName.substring(fullName.lastIndexOf(".") + 1))
      val fieldValues = objArgs.get(path) getOrElse scala.collection.mutable.Map[String, Array[Byte]]();
      fieldValues(fieldName) = splitKV.getValue()
      //Handle imbricated objects without parameter (for example obj.obj2.obj3.id)
      val buffer = new StringBuilder()
      for (i <- 0 until fullName.size) {
        if (fullName(i) == '.') {
          val objName = buffer.toString
          if (!objArgs.contains(objName)) {
            objArgs(buffer.toString) = scala.collection.mutable.Map[String, Array[Byte]]()
          }
        }
        buffer += fullName(i)
      }
      objArgs(path) = fieldValues
      logger.debug("objArgs({}) = {}", path, fieldValues)
    }

    //Build an object of the given class, with the given family (family name is the field name)
    def buildObject(klass: Class[_], family: String, currentField: Field): Any = {
      logger.debug("---------- {}", family)
      if (!objArgs.contains(family)) {
        return getDefaultValue(klass)
      }
      val args = scala.collection.mutable.MutableList[Object]()
      //val mapArgs
      //scan all the object field
      logger.debug("klass={}", klass)
      klass match {
        //Map constructor
        case k if k == MM || k == IM || k == SM => {
          logger.debug(" {} is a map", family)
          //We don't have any data on this company, we return null. //TODO return an empty map instead ?
          if (!objArgs.contains(family)) return null
          //retrieve the map of keys/values from the results
          val paramMap = scala.collection.mutable.Map[String, KeyValue]()
          for ((k, v) <- objArgs.get(family).get) {
            val lastDot = k.lastIndexOf(".")
            val (mapIndice, valueTypeReduced) = lastDot match {
              case -1 => (k.substring(1), k)
              case _ => (k.substring(lastDot + 2), k.substring(lastDot + 1))
            }
            val paramMapVal = if (paramMap.contains(mapIndice)) paramMap(mapIndice) else KeyValue()
            paramMapVal.set(valueTypeReduced, v)
            paramMap(mapIndice) = paramMapVal
          }
          val map = scala.collection.mutable.Map[Any, Any]()
          val hormMap = currentField.getAnnotation(classOf[HormMap])
          val (typeKey, typeValue) = hormMap match {
            case null => (S, S)
            case _ => (hormMap.key(), hormMap.value())
          }
          for ((i, kv) <- paramMap) {
            map(getValue(typeKey, kv.key)) = getValue(typeValue, kv.value)
          }
          k match { case MM => map; case k if k == IM || k == SM => map.toMap }
        }
        //Constructor without parameters (case class without parameter).
        case k if k.getDeclaredFields().isEmpty => {
          klass.newInstance()
        }
        //AnyRef constructor
        case _ => {
          for (field <- klass.getDeclaredFields()) {
            logger.debug("objArgs.get({}).get({})", family, field.getName)
            //If the field exist (AnyRef), getValue will build it.
            if (objArgs.get(family).get.contains(field.getName())) {
              if (logger.isDebugEnabled()) logger.debug("exists{}" + objArgs.get(family).get(field.getName()))
              val value = getValue(field.getType(), objArgs.get(family).get(field.getName())).asInstanceOf[Object]
              args += value
            } else {
              //Else, it might be null or an embbed object, or a map, let's build that.
              args += buildObject(field.getType(), getNexPath(family, field.getName()), field).asInstanceOf[Object]
            }
          }
          if (logger.isDebugEnabled()) {
            logger.debug("construct {}", klass.getName())
            logger.debug("argsNumber: {}", args.size)
            args.foreach(logger.debug("arg: {}", _))
          }
          val constructor = klass.getConstructors.head
          val o = constructor.newInstance(args: _*)
          logger.debug("object constructed {}", o)
          o
        }
      }
    }
    buildObject(klass, "", null)
  }

  //Return the next path (do not add the . if it's the root path)
  private def getNexPath(path: String, name: String) = if (path.length == 0) name else path + "." + name

}