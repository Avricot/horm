package com.avricot.horm

import org.joda.time.DateTime
import org.junit.Test
import org.junit.Assert
import scala.collection.mutable.Map
import org.joda.time.format.ISODateTimeFormat
import org.junit.Ignore

case class User(id: Long, firstname: String, lastname: Int)
case class Trace(id: Array[Byte], category: String, user: User, data: Map[String, String], bool: Boolean) extends HormBaseObject {
  override def getHBaseId() = id
}

object Trace extends HormObject[Trace]

class TraceIntegrationTest {

  @Ignore @Test def read(): Unit = {
    val t = Trace.find(Array[Byte](22))
    println(t)
    Assert.assertTrue(t != None)
    Assert.assertEquals("category", t.get.category)
    Assert.assertEquals(45L, t.get.user.id)
    Assert.assertEquals("firstname", t.get.user.firstname)
    Assert.assertEquals(21, t.get.user.lastname)
    Assert.assertEquals("aqsd", t.get.data.get("a").get)
    Assert.assertEquals("asdqsd", t.get.data.get("asdsf").get)
    Assert.assertEquals(2, t.get.data.size)
    t
  }

  @Ignore @Test def write() = {
    val user = User(45L, "firstname", 21)
    val d1 = new DateTime(15654564L)
    val trace = new Trace(Array[Byte](22), "category", user, Map[String, String]("a" -> "aqsd", "asdsf" -> "asdqsd"), true)
    Trace.save(trace)
  }

  @Ignore @Test def delete() = {
    Trace.delete(Array[Byte](22))
  }

  @Test def writeReadDelete() = {
    HormConfig.init("localhost", 2181)
    write()
    val t1 = read()
    delete()
    val t = Trace.find(Array[Byte](22))
    Assert.assertTrue(t == None)
  }

}