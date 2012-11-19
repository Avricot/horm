package com.avricot.horm.binder.complex

import org.apache.hadoop.hbase.client.Put
import com.avricot.horm.binder.raw.ComplexBinder
import org.slf4j.LoggerFactory
import com.avricot.horm.writer.HormWriter
import com.avricot.horm.reader.HormReader
import scala.collection.mutable.Map
import java.lang.reflect.Field
import com.avricot.horm.binder.raw.RawBinder

object ObjectBinder extends ComplexBinder[Any] {
  val logger = LoggerFactory.getLogger(ObjectBinder.getClass())

  override def read(objArgs: Map[String, Map[String, Array[Byte]]], klass: Class[_], family: String, currentField: Field) = {
    val args = scala.collection.mutable.MutableList[Object]()
    for (field <- klass.getDeclaredFields()) {
      logger.debug("objArgs.get({}).get({})", family, field.getName)
      //If the field exist (AnyRef), getValue will build it.
      if (objArgs.get(family).get.contains(field.getName())) {
        if (logger.isDebugEnabled()) logger.debug("exists{}" + objArgs.get(family).get(field.getName()))
        val value = RawBinder.binders(field.getType()).read(objArgs.get(family).get(field.getName())).asInstanceOf[Object]
        args += value
      } else {
        //Else, it might be null or an embbed object, or a map, let's build that.
        args += HormReader.buildObject(objArgs, field.getType(), HormReader.getNexPath(family, field.getName()), field).asInstanceOf[Object]
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("construct {}", klass.getName())
      logger.debug("argsNumber: {}", args.size)
      args.foreach(logger.debug("arg: {}", _))
    }
    println(klass + "args" + args)
    val constructor = klass.getConstructors.head
    val o = constructor.newInstance(args: _*)
    println(o)
    logger.debug("object constructed {}", o)
    o
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