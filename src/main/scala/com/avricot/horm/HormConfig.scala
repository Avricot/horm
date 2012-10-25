package com.avricot.horm

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.slf4j.LoggerFactory
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.HTable

object HormConfig {
  def logger = LoggerFactory.getLogger(HormConfig.getClass())
  val defaultFamilyNameStr = "data"
  val defaultFamilyName = Bytes.toBytes(defaultFamilyNameStr)

  private var configuration: Configuration = null
  /**
   * Init the hbase connection. Should be called only once during startup.
   */
  def init(zookeeperQuorum: String, zookeeperClientPort: Int) = {
    configuration = HBaseConfiguration.create();
    configuration.setStrings(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum)
    configuration.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperClientPort)
  }

  def getHBaseConf = configuration

  def getTable(tableName: String) = new HTable(configuration, tableName)

  /**
   * Return a hbase admin object base on the conf properties.
   */
  def getHBaseAdmin = new HBaseAdmin(configuration)

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
      newHBaseTable.addFamily(new HColumnDescriptor(defaultFamilyNameStr));
      hbaseAdmin.createTable(newHBaseTable)
      fun()
    }
  }
}