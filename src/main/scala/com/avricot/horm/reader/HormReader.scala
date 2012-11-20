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
import java.lang.reflect.Field
import com.avricot.horm.binder.raw.RawBinder
import scala.collection.mutable.ListBuffer
import com.avricot.horm.binder.complex.ObjectBinder
import com.avricot.horm.binder.raw.ComplexBinder
import com.avricot.horm.HormList
import scala.collection.immutable.List
import scala.collection.immutable.Set

/**
 * Read scala object from hbase.
 */
object HormReader {
  val logger = LoggerFactory.getLogger(HormReader.getClass())

  val S = classOf[String]
  val MM = classOf[scala.collection.mutable.Map[_, _]]
  val IM = classOf[scala.collection.immutable.Map[_, _]]
  val SM = classOf[scala.collection.Map[_, _]]
  val SET = classOf[scala.collection.Set[_]]
  val ISET = classOf[scala.collection.mutable.Set[_]]
  val MSET = classOf[scala.collection.immutable.Set[_]]
  val SEQ = classOf[scala.collection.Seq[_]]
  val ISEQ = classOf[scala.collection.mutable.Seq[_]]
  val MSEQ = classOf[scala.collection.immutable.Seq[_]]

  /**
   * Build an object from a result
   */
  def read(result: Result, klass: Class[_]): Any = {
    if (result.isEmpty()) {
      return None
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

    buildObject(objArgs, klass, "", null)
  }

  //Build an object of the given class, with the given family (family name is the field name)
  def buildObject(objArgs: scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Array[Byte]]], klass: Class[_], family: String, currentField: Field): Any = {
    logger.debug("---------- {}", family)
    if (!objArgs.contains(family)) {
      if (RawBinder.binders.contains(klass)) {
        return RawBinder.binders(klass).default
      }
      if (ComplexBinder.binders.contains(klass)) {
        return ComplexBinder.binders(klass).default
      }
      return null
    }
    //val mapArgs
    //scan all the object field
    logger.debug("klass={}", klass)
    klass match {
      case k if ComplexBinder.binders.contains(k) => ComplexBinder.binders(k).read(objArgs, klass, family, currentField)
      case k if k == SET || k == ISET || k == MSET || k == SEQ || k == ISEQ || k == MSEQ => {

        //We don't have any data on this company, we return null. //TODO return an empty list instead ?
        if (!objArgs.contains(family)) return null
        val paramList = scala.collection.mutable.ListBuffer[Any]()
        val hormList = currentField.getAnnotation(classOf[HormList])
        val klassList = hormList match {
          case null => S
          case _ => hormList.getKlass()
        }
        for ((k, v) <- objArgs.get(family).get) {
          paramList += RawBinder.binders(klassList).read(v)
        }

        k match {
          case k if k == SET || k == ISET => paramList.toSet
          case k if k == MSET => paramList
          case k if k == SEQ || k == ISEQ => paramList.toSeq
          case k if k == MSEQ => scala.collection.mutable.ArraySeq(paramList: _*)
        }
      }
      case k if k == MM || k == IM || k == SM => {
        logger.debug(" {} is a map", family)
        //We don't have any data on this company, we return null. //TODO return an empty map instead ?
        if (!objArgs.contains(family)) return null
        //retrieve the map of keys/values from the results
        val hormMap = currentField.getAnnotation(classOf[HormMap])
        val (typeKey, typeValue) = hormMap match {
          case null => (S, S)
          case _ => (hormMap.key(), hormMap.value())
        }
        val map = scala.collection.mutable.Map[Any, Any]()
        //String as key
        if (typeKey == S) {
          for ((k, v) <- objArgs.get(family).get) {
            map(k) = RawBinder.binders(typeValue).read(v)
          }
        } else {
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

          for ((i, kv) <- paramMap) {
            map(RawBinder.binders(typeKey).read(kv.key)) = RawBinder.binders(typeValue).read(kv.value)
          }
        }
        //Build the correct map type.
        k match {
          case k if k == IM || k == SM => map.toMap
          case k if k == MM => map
          case k if k == classOf[scala.collection.mutable.Map[_, _]] => map;
          case k if k == classOf[scala.collection.mutable.WeakHashMap[_, _]] => scala.collection.mutable.WeakHashMap[Any, Any]() ++ map
          case k if k == classOf[scala.collection.mutable.OpenHashMap[_, _]] => scala.collection.mutable.OpenHashMap[Any, Any]() ++ map
          case k if k == classOf[scala.collection.mutable.LinkedHashMap[_, _]] => scala.collection.mutable.LinkedHashMap[Any, Any]() ++ map
          case k if k == classOf[scala.collection.mutable.ListMap[_, _]] => scala.collection.mutable.ListMap[Any, Any]() ++ map
          case k if k == classOf[scala.collection.mutable.HashMap[_, _]] => scala.collection.mutable.HashMap[Any, Any]() ++ map
          case k if k == classOf[scala.collection.mutable.HashMap[_, _]] => scala.collection.mutable.HashMap[Any, Any]() ++ map
          case k if k == classOf[scala.collection.immutable.HashMap[_, _]] => scala.collection.immutable.HashMap[Any, Any]() ++ map
          case k if k == classOf[scala.collection.immutable.ListMap[_, _]] => scala.collection.immutable.ListMap[Any, Any]() ++ map
        }
      }
      case k if k.getDeclaredFields().isEmpty => {
        klass.newInstance()
      }
      //AnyRef constructor
      case _ => ObjectBinder.read(objArgs, klass, family, currentField)
    }
  }
  //Return the next path (do not add the . if it's the root path)
  def getNexPath(path: String, name: String) = if (path.length == 0) name else path + "." + name

}