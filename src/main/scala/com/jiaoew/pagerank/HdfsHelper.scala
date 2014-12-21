package com.jiaoew.pagerank

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.JobConf

/**
 * Created by jiaoew on 14/12/20.
 */
class HdfsHelper(hdfsPath: String, jobConf: JobConf) {

  def rmr(folder: String) = {
    val path = new Path(folder)
    val fs = FileSystem.get(URI.create(hdfsPath), jobConf)
    fs.deleteOnExit(path)
    println("Delete: " + folder)
    fs.close()
  }

  def mkdirs(folder: String) = {
    val path = new Path(folder)
    val fs = FileSystem.get(URI.create(hdfsPath), jobConf)
    if (!fs.exists(path)) {
      fs.mkdirs(path)
      println("Create: " + folder)
    }
    fs.close()
  }

  def copyFile(local: String, remote: String) = {
    val fs = FileSystem.get(URI.create(hdfsPath), jobConf)
    fs.copyFromLocalFile(new Path(local), new Path(remote))
    println("copy from: " + local + " to " + remote)
    fs.close()
  }

  def rename(src: String, dst: String) = {
    val name1 = new Path(src)
    val name2 = new Path(dst)
    val fs = FileSystem.get(URI.create(hdfsPath), jobConf)
    fs.rename(name1, name2)
    println("Rename: from " + src + " to " + dst)
    fs.close()
  }

}
