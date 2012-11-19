package com.avricot.horm.binder.raw

import org.apache.hadoop.hbase.util.Bytes

/**
 * val binders
 */
object IntBinder extends RawBinder[Int] {
  override def read(param: Array[Byte]) = {
    Bytes.toInt(param)
  }
  override def write(obj: Any): Array[Byte] = {
    Bytes.toBytes(obj.asInstanceOf[Int])
  }
  override def default: Any = 0
}

object LongBinder extends RawBinder[Long] {
  override def read(param: Array[Byte]) = {
    Bytes.toLong(param)
  }
  override def write(obj: Any): Array[Byte] = {
    Bytes.toBytes(obj.asInstanceOf[Long])
  }
  override def default: Any = 0L
}

object FloatBinder extends RawBinder[Float] {
  override def read(param: Array[Byte]) = {
    Bytes.toFloat(param)
  }
  override def write(obj: Any): Array[Byte] = {
    Bytes.toBytes(obj.asInstanceOf[Float])
  }
  override def default: Any = 0.0
}

object StringBinder extends RawBinder[String] {
  override def read(param: Array[Byte]) = {
    Bytes.toString(param)
  }
  override def write(obj: Any): Array[Byte] = {
    Bytes.toBytes(obj.asInstanceOf[String])
  }
}

object BooleanBinder extends RawBinder[Boolean] {
  override def read(param: Array[Byte]) = {
    Bytes.toBoolean(param)
  }
  override def write(obj: Any): Array[Byte] = {
    Bytes.toBytes(obj.asInstanceOf[Boolean])
  }
  override def default: Any = false
}

object ArrayByteBinder extends RawBinder[Array[Byte]] {
  override def read(param: Array[Byte]) = {
    param
  }
  override def write(obj: Any): Array[Byte] = {
    obj.asInstanceOf[Array[Byte]]
  }
}
