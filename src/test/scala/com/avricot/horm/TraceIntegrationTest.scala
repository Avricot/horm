package com.avricot.horm

import org.joda.time.DateTime
import org.junit.Test
import org.junit.Assert
import scala.collection.mutable.Map
import org.joda.time.format.ISODateTimeFormat
import org.junit.Ignore

class TraceIntegrationTest {
  case class User(id: Long, firstname: String, lastname: Int)
  case class Trace(id: Array[Byte], category: String, user: User, data: Map[String, String], bool: Boolean) extends HormBaseObject {
    override def getHBaseId() = id
  }

  object Trace extends HormObject[Trace]

  @Ignore @Test def read(): Unit = {
    val t = Trace.find(Array[Byte](22))
    println(t)
    Assert.assertTrue(t != None)
    Assert.assertEquals("category", t.get.category)
    Assert.assertEquals(45L, t.get.user.id)
    Assert.assertEquals("firstname", t.get.user.firstname)
    Assert.assertEquals(21, t.get.user.lastname)
    Assert.assertEquals("aqsd", t.get.data.get("a"))
    Assert.assertEquals("asdqsd", t.get.data.get("asdsf"))
    t
  }

  @Ignore @Test def write() = {
    val user = User(45L, "userId", 1)
    val d1 = new DateTime(15654564L)
    val trace = new Trace(Array[Byte](22), "category", user, Map[String, String]("a" -> "aqsd", "asdsf" -> "asdqsd"), true)
    Trace.save(trace)
  }

  @Ignore @Test def delete() = {
    Trace.delete(Array[Byte](22))
  }

  @Test def writeReadDelete() = {
    write()
    val t1 = read()
    delete()
    val t = Trace.find(Array[Byte](22))
    Assert.assertTrue(t == None)
  }

}