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
import scala.annotation.target.field
import java.nio.ByteBuffer
import scala.collection.Seq
import scala.annotation.target.field

case class TestMultipleConst @HormConstructor() (test: String, toto: Long, tata: String) extends HormBaseObject {
  def this() = this(null, 0L, null)
  override def getHBaseId() = Array[Byte](22)
}
object TestMultipleConst extends HormObject[TestMultipleConst]

case class TestMultipleConstSmart(test: String, toto: Long, tata: String) extends HormBaseObject {
  def this() = this(null, 0L, null)
  override def getHBaseId() = Array[Byte](22)
}
object TestMultipleConstSmart extends HormObject[TestMultipleConstSmart]

class TraceConstructorIntegrationTest {

  @Test def testMultipleConstructor(): Unit = {
    HormConfig.init("localhost", 2181)
    HormConfig.initTable(classOf[TestMultipleConst])
    val t = TestMultipleConst("aze", 17, "ert")
    TestMultipleConst.save(t)
    val t2 = TestMultipleConst.find(t)
    Assert.assertEquals("aze", t2.get.test)
    Assert.assertEquals("ert", t2.get.tata)
    Assert.assertEquals(17, t2.get.toto)
  }

  @Test def testMultipleConstructorSmart(): Unit = {
    HormConfig.init("localhost", 2181)
    HormConfig.initTable(classOf[TestMultipleConstSmart])
    val t = TestMultipleConstSmart("aze", 17, "ert")
    TestMultipleConstSmart.save(t)
    val t2 = TestMultipleConstSmart.find(t)
    Assert.assertEquals("aze", t2.get.test)
    Assert.assertEquals("ert", t2.get.tata)
    Assert.assertEquals(17, t2.get.toto)
  }

}