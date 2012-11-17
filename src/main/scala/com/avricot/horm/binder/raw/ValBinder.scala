package com.avricot.horm.binder.raw

import org.apache.hadoop.hbase.util.Bytes

/**
 * val binders
 */
object IntBinder extends RawBinder[Int] {
  def read(param: Array[Byte]) = {
    Bytes.toInt(param)
  }
  def write(obj: Any): Array[Byte] = {
    Bytes.toBytes(obj.asInstanceOf[Int])
  }
}

object LongBinder extends RawBinder[Long] {
  def read(param: Array[Byte]) = {
    Bytes.toLong(param)
  }
  def write(obj: Any): Array[Byte] = {
    Bytes.toBytes(obj.asInstanceOf[Long])
  }
}

object FloatBinder extends RawBinder[Float] {
  def read(param: Array[Byte]) = {
    Bytes.toFloat(param)
  }
  def write(obj: Any): Array[Byte] = {
    Bytes.toBytes(obj.asInstanceOf[Float])
  }
}

object StringBinder extends RawBinder[String] {
  def read(param: Array[Byte]) = {
    Bytes.toString(param)
  }
  def write(obj: Any): Array[Byte] = {
    Bytes.toBytes(obj.asInstanceOf[String])
  }
}

object BooleanBinder extends RawBinder[Boolean] {
  def read(param: Array[Byte]) = {
    Bytes.toBoolean(param)
  }
  def write(obj: Any): Array[Byte] = {
    Bytes.toBytes(obj.asInstanceOf[Boolean])
  }
}

object ArrayByteBinder extends RawBinder[Array[Byte]] {
  def read(param: Array[Byte]) = {
    param
  }
  def write(obj: Any): Array[Byte] = {
    obj.asInstanceOf[Array[Byte]]
  }
}
