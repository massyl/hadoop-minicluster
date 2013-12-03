package com.dbtsai.hadoop.util

import java.io.{ FileOutputStream, File }
import org.apache.hadoop.mapred.MiniMRCluster
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster

/**
 * Created with IntelliJ IDEA.
 * User: dbtsai
 * Date: 12/2/13
 * Time: 5:44 PM
 * To change this template use File | Settings | File Templates.
 */

class MiniCluster(dataNodes: Int = 1, taskTrackers: Int = 1) {

  private val configurationFilePath = this.getClass.getProtectionDomain().getCodeSource().getLocation().getPath() + "/hadoop-site.xml"

  var configuration: Option[Configuration] = None
  var miniDFSCluster: Option[MiniDFSCluster] = None
  var miniMRCluster: Option[MiniMRCluster] = None

  def start() {
    System.setProperty("hadoop.log.dir", "build/test/logs")

    val configurationFile = new File(configurationFilePath)
    if (configurationFile.exists()) {
      configurationFile.delete()
    }

    val builder = new MiniDFSCluster.Builder(new Configuration())
    miniDFSCluster = Some(builder.format(true).numDataNodes(dataNodes).build())

    miniMRCluster = Some(new MiniMRCluster(taskTrackers, miniDFSCluster.get.getFileSystem.getUri.toString, 1))

    // Write the necessary config info to hadoop-site.xml
    configuration = Some(miniMRCluster.get.createJobConf()).map(configuration => {
      configuration.setInt("mapred.submit.replication", 2)
      configuration.set("dfs.datanode.address", "0.0.0.0:0")
      configuration.set("dfs.datanode.http.address", "0.0.0.0:0")
      configuration.set("mapred.map.max.attempts", "2")
      configuration.set("mapred.reduce.max.attempts", "2")
      configuration.set("pig.jobcontrol.sleep", "100")
      configuration
    })
    configuration.foreach(_.writeXml(new FileOutputStream(configurationFile)))

    // Set the system properties needed by Pig
    System.setProperty("cluster", configuration.get.get("mapred.job.tracker"))
    System.setProperty("namenode", configuration.get.get("fs.default.name"))
  }

  def shutdown() {
    miniDFSCluster.foreach(_.getFileSystem.close())
    miniDFSCluster.foreach(_.shutdown())
    miniDFSCluster = None
    miniMRCluster.foreach(_.shutdown())
    miniMRCluster = None
    val configurationFile = new File(configurationFilePath)
    if (configurationFile.exists()) {
      configurationFile.delete()
    }
  }
}
