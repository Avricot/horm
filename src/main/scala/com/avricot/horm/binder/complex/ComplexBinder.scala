package com.avricot.horm.binder.raw
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.Map
import org.apache.hadoop.hbase.client.Put
import java.lang.reflect.Field
import com.avricot.horm.reader.KeyValue

object ComplexBinder {
  val binders = Map[Class[_], ComplexBinder[_]]( //Map implementation binding
  /*classOf[scala.collection.Map[_, _]] -> new MapBinder(result = _.toMap),
    classOf[scala.collection.immutable.Map$Map1] -> new MapBinder(result = _.toMap),
    classOf[scala.collection.immutable.Map$Map2] -> new MapBinder(result = _.toMap),
    classOf[scala.collection.immutable.Map$Map3] -> new MapBinder(result = _.toMap),
    classOf[scala.collection.immutable.Map$Map4] -> new MapBinder(result = _.toMap),
    classOf[scala.collection.immutable.Map[_, _]] -> new MapBinder(result = _.toMap),
    classOf[scala.collection.immutable.HashMap[_, _]] -> new MapBinder(() => scala.collection.mutable.WeakHashMap[String, KeyValue](), result = _.toMap),
    classOf[scala.collection.immutable.ListMap[_, _]] -> new MapBinder(() => scala.collection.mutable.ListMap[String, KeyValue](), result = _.toMap),
    classOf[scala.collection.mutable.Map[_, _]] -> new MapBinder(),
    classOf[scala.collection.mutable.WeakHashMap[_, _]] -> new MapBinder(() => scala.collection.mutable.WeakHashMap[String, KeyValue]()),
    classOf[scala.collection.mutable.OpenHashMap[_, _]] -> new MapBinder(() => scala.collection.mutable.OpenHashMap[String, KeyValue]()),
    classOf[scala.collection.mutable.LinkedHashMap[_, _]] -> new MapBinder(() => scala.collection.mutable.LinkedHashMap[String, KeyValue]()),
    classOf[scala.collection.mutable.ListMap[_, _]] -> new MapBinder(() => scala.collection.mutable.ListMap[String, KeyValue]()),
    classOf[scala.collection.mutable.HashMap[_, _]] -> new MapBinder(() => scala.collection.mutable.HashMap[String, KeyValue]())*/ )
}

/**
 * Raw type binder. Bind a class to a unique field.
 */
trait ComplexBinder[T] {
  def read(objArgs: Map[String, Map[String, Array[Byte]]], klass: Class[_], family: String, currentField: Field): T
  def write(family: Array[Byte], fieldName: String, obj: Any, put: Put)
  def default: Any = null
}