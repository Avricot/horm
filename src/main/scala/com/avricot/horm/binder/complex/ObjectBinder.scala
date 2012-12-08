package com.avricot.horm.binder.complex

import org.apache.hadoop.hbase.client.Put
import com.avricot.horm.binder.raw.ComplexBinder
import org.slf4j.LoggerFactory
import com.avricot.horm.writer.HormWriter
import com.avricot.horm.reader.HormReader
import scala.collection.mutable.Map
import java.lang.reflect.Field
import com.avricot.horm.binder.raw.RawBinder
import com.avricot.horm.HormConstructor
import java.lang.reflect.Constructor
import org.apache.commons.collections.map.HashedMap
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedMap
import java.util.concurrent.locks.ReentrantReadWriteLock

object ObjectBinder extends ComplexBinder[Any] {
  val logger = LoggerFactory.getLogger(ObjectBinder.getClass())
  val readWriteLock = new ReentrantReadWriteLock()
  val loadedConstructors = new HashMap[Class[_], Constructor[_]] // with SynchronizedMap[Class[_], Constructor[_]]

  override def read(objArgs: Map[String, Map[String, Array[Byte]]], klass: Class[_], family: String, currentField: Field): Any = {
    val args = scala.collection.mutable.MutableList[Object]()
    for (field <- klass.getDeclaredFields()) {
      logger.debug("objArgs.get({}).get({})", family, field.getName)
      //If the field exist (AnyRef), getValue will build it.
      if (objArgs.get(family).get.contains(field.getName())) {
        if (logger.isDebugEnabled()) logger.debug("val exists{}" + objArgs.get(family).get(field.getName()))
        val value = RawBinder.binders(field.getType()).read(objArgs.get(family).get(field.getName())).asInstanceOf[Object]
        args += value
      } else {
        //Else, it might be null or an embbed object, or a map, let's build that.
        args += HormReader.buildObject(objArgs, field.getType(), HormReader.getNexPath(family, field.getName()), field).asInstanceOf[Object]
      }
    }
    //Try to find the best for constructor this class.
    val constructor = getConstructor(klass, args)
    val o = constructor match {
      case c if c.getParameterTypes().size == 0 => {
        constructor.newInstance()
        //TODO : add the args using field getter and setters.
      }
      case _ => {
        if (logger.isDebugEnabled()) {
          logger.debug("construct {}", klass.getName())
          logger.debug("argsNumber: {}", args.size)
          args.foreach(logger.debug("arg: {}", _))
          constructor.getParameterTypes().foreach(logger.debug("expected arg: {}", _))
        }
        constructor.newInstance(args: _*)
      }
    }
    logger.debug("object constructed {}", o)
    o
  }

  override def write(family: Array[Byte], fieldName: String, field: Field, obj: Any, put: Put) = {
    logger.debug("exploring object {} ", obj.getClass)
    for (f <- obj.getClass.getDeclaredFields) {
      f.setAccessible(true)
      val t = f.getType()
      val value = f.get(obj)
      //remove $outer access
      if (value != null && !f.getName().startsWith("$")) {
        HormWriter.findType(family, f.getName(), f, value, put)
      }
    }
  }

  /**
   * Try to get the best constructor for a class as following.
   * Cache the value in a sychronized map, so it's supposed to be fast.
   * - if single constructor, return it. <br />
   * - if HormConstructor, return it.<br />
   * - if a constructor with the same paramater type than args, return it.<br />
   * - if a default constructor (0 parameter)<br />
   * - log an error and return null (shouldn't happen).
   *
   * ReadWriteLock pattern. Do not garantee that the constructor won't be calculated twice or more, but we don't care.
   */
  def getConstructor(klass: Class[_], args: scala.collection.mutable.MutableList[Object]): Constructor[_] = {
    readWriteLock.readLock().lock()
    try {
      val cachedConstructor = loadedConstructors.get(klass)
    } finally {
      readWriteLock.readLock().unlock()
    }

    loadedConstructors.get(klass) match {
      case Some(opt) => opt
      case _ => {
        readWriteLock.writeLock().lock()
        try {
          val constructor = klass.getConstructors match {
            case constructors if constructors.size == 1 => constructors.head
            case constructors => {
              //First try to find a constructor with the HormConstructor annotation
              constructors.find(_.getAnnotation(classOf[HormConstructor]) != null) match {
                case None => {
                  def getDefaultConstructor() = {
                    constructors.find(_.getParameterTypes().size == 0) match {
                      case None => logger.error("can't find any valid constructor for class {}" + klass); null;
                      case opt => opt.get
                    }
                  }
                  //If HormConstructor can't be find, try to find a constructor with the same number of parameters
                  if (args == null) {
                    //No args, pick the default constructor.
                    getDefaultConstructor()
                  } else {
                    constructors.find({
                      _.getParameterTypes() match {
                        //The size might match, we check the argument type.
                        case params if params.size == args.size => {
                          for (i <- 0 to params.size - 1) {
                            if (!params(i).isInstance(args(i))) {
                              false
                            }
                          }
                          true
                        }
                        case _ => false
                      }
                    }) match {
                      //Pick the default constructor (no params).
                      case None => getDefaultConstructor()
                      case opt => opt.get
                    }
                  }
                }
                //HormConstructor annotation
                case opt => opt.get
              }
            }
          }
          loadedConstructors.put(klass, constructor)
          constructor
        } finally {
          readWriteLock.writeLock().unlock()
        }
      }
    }
  }
}