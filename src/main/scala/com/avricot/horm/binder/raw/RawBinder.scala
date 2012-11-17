package com.avricot.horm.binder.raw
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.Map

object RawBinder {
  val binders = Map[Class[_], RawBinder[_]](
    classOf[Int] -> IntBinder,
    classOf[java.lang.Integer] -> IntBinder,
    classOf[Long] -> LongBinder,
    classOf[java.lang.Long] -> LongBinder,
    classOf[Float] -> FloatBinder,
    classOf[java.lang.Float] -> FloatBinder,
    classOf[String] -> StringBinder,
    classOf[Boolean] -> BooleanBinder,
    classOf[java.lang.Boolean] -> BooleanBinder,
    classOf[Array[Byte]] -> ArrayByteBinder)
}

/**
 * Raw type binder. Bind a class to a unique field.
 */
trait RawBinder[T] {
  def read(param: Array[Byte]): T
  def write(obj: Any): Array[Byte]
  def default = null
}