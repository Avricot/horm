package com.avricot.horm.binder.raw
import scala.collection.mutable.Map

object RawBinder {
  val binders = Map[Class[_], RawBinder[_]]()
}

/**
 * Raw type binder. Bind a class to a unique field.
 */
trait RawBinder[T] {
  def read(param: Array[Byte]): T
  def write(obj: Any): Array[Byte]
}