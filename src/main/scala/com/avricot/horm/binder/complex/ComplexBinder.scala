package com.avricot.horm.binder.raw
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.Map
import org.apache.hadoop.hbase.client.Put
import java.lang.reflect.Field
import com.avricot.horm.reader.KeyValue

object ComplexBinder {
  val binders = Map[Class[_], ComplexBinder[_]]()
}

/**
 * Raw type binder. Bind a class to a unique field.
 */
trait ComplexBinder[T] {
  def read(objArgs: Map[String, Map[String, Array[Byte]]], klass: Class[_], family: String, currentField: Field): T
  def write(family: Array[Byte], fieldName: String, field: Field, obj: Any, put: Put)
  def default: Any = null
}