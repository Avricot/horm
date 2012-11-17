package com.avricot.horm.binder.raw
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.Map
import org.apache.hadoop.hbase.client.Put
import com.avricot.horm.binder.complex.MapBinder

object ComplexBinder {
  val binders = Map[Class[_], ComplexBinder[_]](
    classOf[scala.collection.immutable.Map[_, _]] -> MapBinder,
    classOf[scala.collection.mutable.Map[_, _]] -> MapBinder,
    classOf[scala.collection.mutable.WeakHashMap[_, _]] -> MapBinder,
    classOf[scala.collection.mutable.OpenHashMap[_, _]] -> MapBinder,
    classOf[scala.collection.mutable.LinkedHashMap[_, _]] -> MapBinder,
    classOf[scala.collection.mutable.ListMap[_, _]] -> MapBinder,
    classOf[scala.collection.mutable.MultiMap[_, _]] -> MapBinder,
    classOf[scala.collection.mutable.HashMap[_, _]] -> MapBinder)
}

/**
 * Raw type binder. Bind a class to a unique field.
 */
trait ComplexBinder[T] {
  def read(param: Array[Byte]): T
  def write(family: Array[Byte], fieldName: String, obj: Any, put: Put)
  def default = null
}