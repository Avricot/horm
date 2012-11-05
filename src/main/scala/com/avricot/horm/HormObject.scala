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
import org.slf4j.LoggerFactory
import java.lang.reflect.Type
import scala.collection.mutable.MutableList
import scala.collection.mutable.LinkedList
import scala.collection.mutable.ArrayBuffer
import com.avricot.horm.reader.HormReader
import com.avricot.horm.writer.HormWriter

/**
 * Default hbase trait.
 */
trait HormBaseObject {
  def getHBaseId(): Array[Byte]
}
/**
 * Default hbase object, with hbase helpers for model companions.
 */
class HormObject[A <: HormBaseObject](tabName: String = null) {
  val logger = LoggerFactory.getLogger(HormConfig.getClass())
  //Get the class of A
  val persistentClass = getClass().getGenericSuperclass().asInstanceOf[ParameterizedType].getActualTypeArguments()(0).asInstanceOf[Class[A]];
  val tableName = if (tabName == null) persistentClass.getSimpleName().toLowerCase() else tabName

  val config = HBaseConfiguration.create()
  val table = HormConfig.getTable(tableName)
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
    val put = HormWriter.write(defaultPath, obj)
    table.put(put)
  }

  /**
   * Build an object from a result
   */
  def getFromResult(result: Result): Option[A] = {
    var r = HormReader.read(result, persistentClass)
    r match {
      case None => None
      case _ => Option[A](r.asInstanceOf[A])
    }
  }
}