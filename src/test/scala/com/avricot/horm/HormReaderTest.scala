package com.avricot.horm

import org.joda.time.DateTime
import org.junit.Test
import org.junit.Assert
import scala.collection.mutable.Map
import org.joda.time.format.ISODateTimeFormat
import org.junit.Ignore
import com.avricot.horm.reader.KeyValue

class HormReaderTest {
  @Test def writeReadDelete() = {
    val kv = KeyValue()
    kv.set("k", Array[Byte](2))
    Assert.assertEquals(2, kv.key(0))
    Assert.assertEquals(null, kv.value)
    kv.set("v", Array[Byte](3))
    Assert.assertEquals(3, kv.value(0))
    Assert.assertEquals(2, kv.key(0))
  }

}