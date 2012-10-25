package com.avricot.horm

import org.apache.hadoop.hbase.HBaseConfiguration
import java.security.MessageDigest
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTime
import org.apache.hadoop.hbase.client.HTable
import scala.Array.canBuildFrom
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Delete
import java.util.ArrayList
import org.apache.hadoop.hbase.client.Scan
import java.lang.reflect.ParameterizedType

/**
 * Default hbase trait.
 */
trait BaseHBaseObject {
  def getHBaseId(): Array[Byte]
}

object HBaseObject {
  val defaultFamilyNameStr = "data"
  val defaultFamilyName = Bytes.toBytes(defaultFamilyNameStr)
}
/**
 * Default hbase object, with hbase helpers for model companions.
 */
class HBaseObject[A <: BaseHBaseObject](name: String) {
  //Get the class of A
  val persistentClass = getClass().getGenericSuperclass().asInstanceOf[ParameterizedType].getActualTypeArguments()(0).asInstanceOf[Class[A]];
  val tableName = name
  val config = HBaseConfiguration.create()
  val table = new HTable(config, tableName);
  val defaultPath = Bytes.toBytes("")

  val isoFormatter = ISODateTimeFormat.dateTime();

  def sha1(string: String) = {
    val md = MessageDigest.getInstance("SHA-1")
    md.update(string.getBytes)
    md.digest().map(i => "%02x".format(i)).mkString
  }

  /**
   * Scan all the table and return all the entities.
   */
  def findAll(): List[A] = {
    val rs = table.getScanner(new Scan)
    scan(rs, getFromResult(_))
  }

  /**
   * Return an object by it's id.
   */
  def find(id: Array[Byte]): Option[A] = {
    val get = new Get(id)
    val result = table.get(get)
    getFromResult(result)
  }

  /**
   * Return an object by it's id.
   */
  def find(id: String): Option[A] = {
    find(Bytes.toBytes(id))
  }

  /**
   * Delete an object by it's id.
   */
  def delete(id: String): Unit = {
    delete(Bytes.toBytes(id))
  }

  /**
   * Delete an object by it's id.
   */
  def delete(id: Array[Byte]): Unit = {
    val del = new Delete(id)
    table.delete(del)
  }

  /**
   * Delete objects by their ids.
   */
  def delete(ids: Array[Byte]*): Unit = {
    val dels = new ArrayList[Delete]()
    for (id <- ids) {
      dels.add(new Delete(id))
    }
    table.delete(dels)
  }

  /**
   * Scan a table, execute the given fun to retrive the A and return a list of the values.
   */
  def scan(rs: ResultScanner, fun: (Result) => Option[A]): List[A] = rs.next() match {
    case null => Nil
    case r =>
      fun(r).get :: scan(rs, fun)
  }

  /**
   * Return a String from the given column.
   */
  def getStr(rs: Result, columFamily: String, column: String): String = {
    Bytes.toString(getByte(rs, columFamily, column))
  }

  /**
   * Return a [Byte] from the given column.
   */
  def getByte(rs: Result, columFamily: String, column: String): Array[Byte] = {
    rs.getValue(Bytes.toBytes("info"), Bytes.toBytes("id"))
  }

  /**
   * Shortcut to add values to the given put.
   */
  def add(put: Put, family: String, qualifier: String, value: String) = {
    put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value))
  }

  /**
   * Save the given object to the db.
   * Recursivly explore imbricated object.
   */
  def save(obj: A) = {
    val put = new Put(obj.getHBaseId)
    //Convert the value to an Array[Byte] and add it to the put.
    def findType(path: Array[Byte], name: String, value: Any): Unit = {
      val bytes = value match {
        case v if v == null => null
        case v: Int => Bytes.toBytes(v.asInstanceOf[Int])
        case v: Long => Bytes.toBytes(v.asInstanceOf[Long])
        case v: Float => Bytes.toBytes(v.asInstanceOf[Float])
        case v: String => Bytes.toBytes(v.asInstanceOf[String])
        case v: Boolean => Bytes.toBytes(v.asInstanceOf[Boolean])
        case v: DateTime => Bytes.toBytes(isoFormatter.print(v.asInstanceOf[DateTime]))
        case v: Array[Byte] => v.asInstanceOf[Array[Byte]]
        case v: scala.collection.mutable.HashMap[_, _] => {
          println("ok i'm a map")
          for ((k, v) <- v.asInstanceOf[scala.collection.mutable.HashMap[_, _]]) {
            println(name + " " + k.toString + ", " + v)
            findType(getNexPath(path, name), k.toString, v)
          }
          null
        }
        case v: Any => exploreObj(getNexPath(path, name), v); null
        case _ => println("error type is not supported for name " + name + " and path " + Bytes.toString(path)); null
      }
      if (bytes != null) {
        println(path.size + "put.add(" + Bytes.toString(getNexPath(path, name)) + ", " + bytes + "))")
        put.add(HBaseObject.defaultFamilyName, getNexPath(path, name), bytes)
      }
    }

    //Explore an object using reflection and save all its type one by one, recursivly.
    def exploreObj(family: Array[Byte], obj: Any) = {
      println("exploring(" + Bytes.toString(family) + ", " + obj.getClass)
      for (field <- obj.getClass.getDeclaredFields) {
        field.setAccessible(true)
        val t = field.getType()
        val value = field.get(obj)
        findType(family, field.getName(), value)
      }
    }
    exploreObj(defaultPath, obj)
    table.put(put)
  }

  //Return the next path (do not add the . if it's the root path)
  private def getNexPath(path: Array[Byte], name: String) = if (path.length == 0) Bytes.toBytes(name) else Bytes.toBytes(Bytes.toString(path) + "." + name)
  private def getNexPath(path: String, name: String) = if (path.length == 0) name else path + "." + name

  val I = classOf[Int]
  val L = classOf[Long]
  val F = classOf[Float]
  val S = classOf[String]
  val B = classOf[Boolean]
  val D = classOf[DateTime]
  val A = classOf[Array[Byte]]
  val M = classOf[scala.collection.mutable.Map[_, _]]
  /**
   * Build an object from a result
   */
  def getFromResult(result: Result): Option[A] = {
    if (result.isEmpty()) {
      return None
    }
    val klass = A.getClass()

    //Return the value from the given Array[Byte]
    def getValue(klass: Class[_], fieldValue: Array[Byte]) = {
      klass match {
        case I => Bytes.toInt(fieldValue)
        case L => Bytes.toLong(fieldValue)
        case F => Bytes.toFloat(fieldValue)
        case S => Bytes.toString(fieldValue)
        case B => Bytes.toBoolean(fieldValue)
        case D => isoFormatter.parseDateTime(Bytes.toString(fieldValue))
        case A => fieldValue
        case _ => println("Damn, i'm nothing known !" + klass); null
      }
    }

    //Init and sort the data by column family.
    val objArgs = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Array[Byte]]]();
    for (kv <- result.raw()) {
      val splitKV = kv.split()
      val fullName = Bytes.toString(splitKV.getQualifier())
      val containsDot = fullName.contains(".")
      val (path, fieldName) = if (!containsDot) ("", fullName) else (fullName.substring(0, fullName.lastIndexOf(".")), fullName.substring(fullName.lastIndexOf(".") + 1))
      val fieldValues = objArgs.get(path) getOrElse scala.collection.mutable.Map[String, Array[Byte]]();
      fieldValues(fieldName) = splitKV.getValue()
      objArgs(path) = fieldValues
    }

    //Build an object of the given class, with the given family (family name is the field name)
    def buildObject(klass: Class[_], family: String): Any = {
      println("----------" + family)
      if (!objArgs.contains(family)) return null
      val args = scala.collection.mutable.MutableList[Object]()
      //val mapArgs
      //scan all the object field
      println("klass=" + klass)
      klass match {
        //Map constructor
        case M => {
          println("ok i'm a map family " + family)
          val map = scala.collection.mutable.Map[String, String]();
          //We don't have any data on this company, we return null. //TODO return an empty map instead ?
          if (!objArgs.contains(family)) return null
          //Build the map from the results
          for ((k, v) <- objArgs.get(family).get) {
            map(k) = Bytes.toString(v)
          }
          map
        }
        //AnyRef constructor
        case _ => {
          for (field <- klass.getDeclaredFields()) {
            println("objArgs.get(" + family + ").get(" + field.getName() + ")")
            //If the field exist (AnyRef), getValue will build it.
            if (objArgs.get(family).get.contains(field.getName())) {
              println("existst" + objArgs.get(family).get(field.getName()))
              val value = getValue(field.getType(), objArgs.get(family).get(field.getName())).asInstanceOf[Object]
              args += value
            } else { //Else, it might be null or an embbed object, or a map, let's build that.
              args += buildObject(field.getType(), getNexPath(family, field.getName())).asInstanceOf[Object]
            }
          }
          println("construct " + klass.getName())
          val constructor = klass.getConstructors.head
          constructor.newInstance(args: _*)
        }
      }
    }
    Option[A](buildObject(persistentClass, "").asInstanceOf[A])
  }
}