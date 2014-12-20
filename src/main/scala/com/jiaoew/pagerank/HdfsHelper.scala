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

  def rename(src: String, dst: String) = {
    val name1 = new Path(src)
    val name2 = new Path(dst)
    val fs = FileSystem.get(URI.create(hdfsPath), jobConf)
    fs.rename(name1, name2)
    println("Rename: from " + src + " to " + dst)
    fs.close()
  }

}
