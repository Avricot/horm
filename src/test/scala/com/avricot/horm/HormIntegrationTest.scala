package com.avricot.horm

import org.joda.time.DateTime
import org.junit.Test
import org.junit.Assert
import scala.collection.mutable.Map
import org.joda.time.format.ISODateTimeFormat
import org.junit.Ignore
import annotation.target.field
import scala.annotation.target.field
import scala.reflect.BeanProperty
import scala.annotation.target.field  

case class User(id: Long, firstname: String, lastname: Int)
case class Trace(id: Array[Byte], category: String, user: User,  @(HormMap @field )(key=classOf[String], value=classOf[Int]) data: Map[String, Int],  @(HormMap @field )(key=classOf[Boolean], value=classOf[Long]) @BeanProperty imudata: scala.collection.immutable.Map[Boolean, Long], bool: Boolean) extends HormBaseObject {
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
    Assert.assertEquals(2, t.get.data.get("a").get)
    Assert.assertEquals(4, t.get.data.get("asdsf").get)
    Assert.assertEquals(2, t.get.data.size)
    Assert.assertEquals(1L, t.get.imudata.get(false).get)
    Assert.assertEquals(1, t.get.imudata.size)
    t
  }

  @Ignore @Test def write() = {
    val user = User(45L, "firstname", 21)
    val d1 = new DateTime(15654564L)
    val trace = new Trace(Array[Byte](22), "category", user, Map[String, Int]("a" -> 2, "asdsf" -> 4), scala.collection.immutable.Map[Boolean, Long](false -> 1L), true)
    Trace.save(trace)
  }

  @Ignore @Test def delete() = { 
    Trace.delete(Array[Byte](22)) 
  }

  @Test def writeReadDelete() = {
    val user = User(45L, "firstname", 21)
    val d1 = new DateTime(15654564L)

    val trace = new Trace(Array[Byte](22), "category", user, Map[String, Int]("a" -> 2, "asdsf" -> 4), scala.collection.immutable.Map[Boolean, Long](false -> 1L), true)
      
    HormConfig.init("localhost", 2181)
    HormConfig.initTable(classOf[Trace])
    write()
    val t1 = read()
    delete()
    val t = Trace.find(Array[Byte](22))
    Assert.assertTrue(t == None)
  }

}