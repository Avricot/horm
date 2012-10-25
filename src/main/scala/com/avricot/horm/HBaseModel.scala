package com.avricot.horm

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.HBaseAdmin

import org.slf4j.LoggerFactory
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor

object HBaseModel {
  def logger = LoggerFactory.getLogger(HBaseModel.getClass())
  private var zookeeperQuorum: String = "localhost"
  private var zookeeperClientPort: Int = 2181

  /**
   * Init the hbase connection. Should be called only once during startup.
   */
  def init(zookeeperQuorum: String, zookeeperClientPort: Int) = {
    this.zookeeperQuorum = zookeeperQuorum
    this.zookeeperClientPort = zookeeperClientPort
  }

  /**
   * Return a hbase admin object base on the conf properties.
   */
  def getHBaseAdmin() = {
    val configuration = HBaseConfiguration.create();
    configuration.setStrings(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum)
    configuration.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperClientPort)
    new HBaseAdmin(configuration)
  }

  /**
   * Init the given table (create it if doesn't exist).
   */
  def initTable(klass: Class[_]) {
    initTable(klass, () => Unit)
  }

  /**
   * Init the given table (create it if doesn't exist), and execute the fun if the table doesn't exist.
   */
  def initTable(klass: Class[_], fun: () => Unit) {
    val hbaseAdmin = getHBaseAdmin
    val tableName = klass.getSimpleName().toLowerCase()
    if (hbaseAdmin.tableExists(tableName)) {
      logger.info("table {} already exists", tableName)
    } else {
      logger.info("table {} doesn't exist, try to create it", tableName)
      //Try to find all the column families of the object
      val newHBaseTable = new HTableDescriptor(tableName);
      newHBaseTable.addFamily(new HColumnDescriptor(HBaseObject.defaultFamilyNameStr));
      hbaseAdmin.createTable(newHBaseTable)
      fun()
    }
  }
}