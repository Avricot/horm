package com.avricot.horm

import org.joda.time.DateTime
import org.junit.Test
import org.junit.Assert
import scala.collection.Map
import org.joda.time.format.ISODateTimeFormat
import org.junit.Ignore
import annotation.target.field
import scala.annotation.target.field
import scala.reflect.BeanProperty
import java.nio.ByteBuffer
import scala.collection.Seq

case class SimpleObj(id: Array[Byte], m: Map[String, String])
case class SimpleObjWrapper(obj: SimpleObj)
case class DoubleObjWrapper(obj: SimpleObjWrapper)

case class TripleObjWrapper(obj: DoubleObjWrapper) extends HormBaseObject {
  override def getHBaseId() = obj.obj.obj.id
}
case class ListTest(muLis: Seq[String]) extends HormBaseObject {
  override def getHBaseId() = Array[Byte](22)
}
object ListTest extends HormObject[ListTest]

object TripleObjWrapper extends HormObject[TripleObjWrapper]

case class User(id: Long, firstname: String, lastname: Int)
case class TraceContent(trace: Trace) extends HormBaseObject {
  override def getHBaseId() = trace.id
}
case class Trace(id: Array[Byte], treeMap: scala.collection.immutable.TreeMap[String, String], category: String, user: User, @(HormMap @field)(key = classOf[String], value = classOf[Int]) data: scala.collection.mutable.Map[String, Int], @(HormMap @field)(key = classOf[Boolean], value = classOf[Long]) imudata: scala.collection.immutable.Map[Boolean, Long], bool: Boolean, date: DateTime = null)

object TraceContent extends HormObject[TraceContent]

class TraceIntegrationTest {

  @Test def testList(): Unit = {
    HormConfig.init("localhost", 2181)
    HormConfig.initTable(classOf[ListTest])
    val t = ListTest(Seq("aze", "ert"))
    ListTest.save(t)
    val t2 = ListTest.find(t)
    Assert.assertEquals("aze", t2.get.muLis(0))
    Assert.assertEquals("ert", t2.get.muLis(1))
  }

  @Test def crudWrapper(): Unit = {
    HormConfig.init("localhost", 2181)
    HormConfig.initTable(classOf[TripleObjWrapper])
    TripleObjWrapper.delete(Array[Byte](22))
    val t = TripleObjWrapper(DoubleObjWrapper(SimpleObjWrapper(SimpleObj(Array[Byte](22), Map[String, String]("a" -> "aa")))))
    TripleObjWrapper.save(t)
    val t2 = TripleObjWrapper.find(Array[Byte](22))
    Assert.assertTrue(t2.isDefined)
    Assert.assertArrayEquals(Array[Byte](22), t2.get.getHBaseId)
    Assert.assertEquals("aa", t2.get.obj.obj.obj.m("a"))
    TripleObjWrapper.delete(Array[Byte](22))
  }

  @Ignore @Test def read(): Unit = {
    val t = TraceContent.find(Array[Byte](22))
    println(t)
    Assert.assertTrue(t != None)
    Assert.assertEquals("category", t.get.trace.category)
    Assert.assertEquals(45L, t.get.trace.user.id)
    Assert.assertEquals("firstnamée", t.get.trace.user.firstname)
    Assert.assertEquals(21, t.get.trace.user.lastname)
    Assert.assertEquals(2, t.get.trace.data.get("a").get)
    Assert.assertEquals(4, t.get.trace.data.get("asdsf").get)
    Assert.assertEquals(2, t.get.trace.data.size)
    Assert.assertEquals(1L, t.get.trace.imudata.get(false).get)
    Assert.assertEquals(1, t.get.trace.imudata.size)
    val it = t.get.trace.treeMap.iterator
    val emptyValue = it.next
    Assert.assertEquals("aa", emptyValue._1)
    Assert.assertEquals("", emptyValue._2)
    Assert.assertEquals("ee", it.next._1)
    val nullValue = it.next
    Assert.assertEquals("oo", nullValue._1)
    Assert.assertEquals(null, nullValue._2)
    Assert.assertEquals("zz", it.next._1)
    t
  }

  @Ignore @Test def write(): Unit = {
    val user = User(45L, "firstnamée", 21)
    val d1 = new DateTime(15654564L)
    val trace = TraceContent(new Trace(Array[Byte](22), scala.collection.immutable.TreeMap("ee" -> "lkjqsd", "zz" -> "sdf", "aa" -> "", "oo" -> null), "category", user, scala.collection.mutable.Map[String, Int]("a" -> 2, "asdsf" -> 4), scala.collection.immutable.Map[Boolean, Long](false -> 1L), true))
    TraceContent.save(trace)
  }

  @Ignore @Test def delete(): Unit = {
    TraceContent.delete(Array[Byte](22))
  }

  @Test def writeReadDelete() = {
    HormConfig.init("localhost", 2181)
    HormConfig.initTable(classOf[TraceContent])
    write()
    val t1 = read()
    delete()
    val t = TraceContent.find(Array[Byte](22))
    Assert.assertTrue(t == None)
  }

}