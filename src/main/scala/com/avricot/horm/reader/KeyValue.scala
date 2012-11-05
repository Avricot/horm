package com.avricot.horm.reader

/**
 * A key/value container used during reading map key/values from hbase.
 */
case class KeyValue(var key: Array[Byte] = null, var value: Array[Byte] = null) {
  def set(valueTypeReduced: String, value: Array[Byte]) = {
    if (valueTypeReduced(0) == 'k') {
      key = value
    } else {
      this.value = value
    }
  }
}
