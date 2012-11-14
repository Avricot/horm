package com.avricot.horm

import org.joda.time.DateTime
import org.junit.Test
import org.junit.Assert
import scala.collection.mutable.Map
import org.joda.time.format.ISODateTimeFormat
import org.junit.Ignore
import com.avricot.horm.reader.KeyValue
import java.nio.ByteBuffer
import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer

case class ScanTrace(id: Array[Byte], test: String) extends HormBaseObject {
  override def getHBaseId() = id
}

object ScanTrace extends HormObject[ScanTrace]

class HormScanIntegrationTest {

  @Test def SanSync(): Unit = {
    val time = System.currentTimeMillis()
    val ids = fillHBase(time)

    val results = ScanTrace.scanSynch(ByteBuffer.allocate(8).putLong(time + 3).array(), ByteBuffer.allocate(8).putLong(time + 4).array(), trace => {
      Option(trace)
    })
    Assert.assertEquals(1, results.size)
    Assert.assertArrayEquals(ByteBuffer.allocate(9).put(1.asInstanceOf[Byte]).putLong(time + 3).array(), results.head.id)

    //Cleanup the database.
    ids.foreach(ScanTrace.delete(_))
  }

  @Test def SanSyncWithNoneResult(): Unit = {
    val time = System.currentTimeMillis()
    val ids = fillHBase(time)

    val results = ScanTrace.scanSynch(ByteBuffer.allocate(8).putLong(time + 3).array(), ByteBuffer.allocate(8).putLong(time + 4).array(), trace => {
      None
    })

    Assert.assertEquals(0, results.size)

    //Cleanup the database.
    ids.foreach(ScanTrace.delete(_))
  }

  @Test def executeAction(): Unit = {
    val time = System.currentTimeMillis()
    val ids = fillHBase(time)
    val results = ScanTrace.scan(ByteBuffer.allocate(8).putLong(time + 3).array(), ByteBuffer.allocate(8).putLong(time + 4).array(), trace => {
      Option(trace)
    })
    Assert.assertEquals(1, results.size)
    Assert.assertArrayEquals(ByteBuffer.allocate(9).put(1.asInstanceOf[Byte]).putLong(time + 3).array(), results.head.id)

    //Cleanup the database.
    ids.foreach(ScanTrace.delete(_))
  }

  private def fillHBase(time: Long): List[Array[Byte]] = {
    HormConfig.init("localhost", 2181)
    HormConfig.initTable(classOf[ScanTrace])
    val ids = List(
      ByteBuffer.allocate(9).put(0.asInstanceOf[Byte]).putLong(time + 1).array(),
      ByteBuffer.allocate(9).put(1.asInstanceOf[Byte]).putLong(time + 2).array(),
      ByteBuffer.allocate(9).put(1.asInstanceOf[Byte]).putLong(time + 3).array(),
      ByteBuffer.allocate(9).put(2.asInstanceOf[Byte]).putLong(time + 6).array(),
      ByteBuffer.allocate(9).put(3.asInstanceOf[Byte]).putLong(time + 7).array())

    ids.foreach(id => { ScanTrace.save(new ScanTrace(id, "aaa")) })
    ids
  }
  @Test def scanWithRegionEmpty(): Unit = {
    val time = System.currentTimeMillis()
    val results = ScanTrace.scanWithRegion(ByteBuffer.allocate(8).putLong(time + 3).array(), ByteBuffer.allocate(8).putLong(time + 6).array())
    Assert.assertTrue(results.isEmpty)
  }

  @Test def scanWithRegion(): Unit = {
    HormConfig.init("localhost", 2181)
    HormConfig.initTable(classOf[ScanTrace])
    val time = System.currentTimeMillis()
    val ids = List(
      ByteBuffer.allocate(9).put(0.asInstanceOf[Byte]).putLong(time + 1).array(),
      ByteBuffer.allocate(9).put(1.asInstanceOf[Byte]).putLong(time + 2).array(),
      ByteBuffer.allocate(9).put(1.asInstanceOf[Byte]).putLong(time + 3).array(),
      ByteBuffer.allocate(9).put(2.asInstanceOf[Byte]).putLong(time + 4).array(),
      ByteBuffer.allocate(9).put(2.asInstanceOf[Byte]).putLong(time + 5).array(),
      ByteBuffer.allocate(9).put(2.asInstanceOf[Byte]).putLong(time + 6).array(),
      ByteBuffer.allocate(9).put(3.asInstanceOf[Byte]).putLong(time + 7).array(),
      ByteBuffer.allocate(13).put(0.asInstanceOf[Byte]).putLong(time + 4).putInt(998).array(),
      ByteBuffer.allocate(13).put(1.asInstanceOf[Byte]).putLong(time + 4).putInt(999).array())
    for (id <- ids) {
      val trace = new ScanTrace(id, "aaa")
      ScanTrace.save(trace)
    }
    val results = ScanTrace.scanWithRegion(ByteBuffer.allocate(8).putLong(time + 3).array(), ByteBuffer.allocate(8).putLong(time + 6).array())

    def idAsString(st: ScanTrace): String = {
      val bb = ByteBuffer.wrap(st.id);
      val idStr = bb.get() + " " + bb.getLong()
      idStr
    }

    Assert.assertEquals("1 " + (time + 3), idAsString(results(0)))
    Assert.assertEquals("2 " + (time + 4), idAsString(results(1)))
    Assert.assertEquals("0 " + (time + 4), idAsString(results(2)))
    Assert.assertEquals("1 " + (time + 4), idAsString(results(3)))
    Assert.assertEquals("2 " + (time + 5), idAsString(results(4)))
    Assert.assertEquals(5, results.size)

    //Cleanup the database.
    for (r <- results) {
      ScanTrace.delete(r.id)
    }
  }

}