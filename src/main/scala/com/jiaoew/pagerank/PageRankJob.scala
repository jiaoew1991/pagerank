package com.jiaoew.pagerank

import java.lang.Iterable
import java.text.DecimalFormat
import java.util
import java.util.regex.Pattern

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Reducer, Mapper}
import org.apache.hadoop.security.UserGroupInformation

import scala.collection.JavaConversions._
import scala.util._
import scala.util.control.NonFatal

/**
 * Created by jiaoew on 14/12/19.
 */
class PageMapper extends Mapper[LongWritable, Text, Text, Text] {

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    val tokens = PageRankJob.DELIMITER.split(value.toString())
    val k = new Text(tokens(0))
    val v = new Text(tokens(1))
    context.write(k, v)
    println(s"key: $k, values: $v")
  }
}
import PageRankJob._
class PageReducer extends Reducer[Text, Text, Text, Text] {
  override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val G = Array.fill(PAGE_NUMS)((1 - D) / PAGE_NUMS) // 概率矩阵列
    val A = Array.fill(PAGE_NUMS)(0.0f) // 近邻矩阵列
    values foreach { pair =>
      val idx = pair.toString.toInt
      println(s"value index $idx")
      A(idx - 1) = 1
    }
    val sum = A.count(_ == 1).max(1) // 分母不能为0

    val strs = G.zipWithIndex map { pair =>
      (pair._1 + D * A(pair._2)) / sum toString
    }
    val output = new Text(strs.mkString(","))
    println(key + ":" + output.toString())
    context.write(key, output)
  }
}
object PageRankJob {

  case class HdfsPathConfig(page: String, pagerank: String, input: String, inputPr: String, tmpMatrix: String, tmpPr: String, result: String)

  val DELIMITER = Pattern.compile("[\t,]")
  val PAGE_NUMS = 4
  val D = 0.85
  val HDFS = "hdfs://omgthree.cloudapp.net:54310"

  def scaleFloat(f: Float) = {// 保留6位小数
    val df = new DecimalFormat("##0.000000")
    df.format(f)
  }
  def config() = {
    val conf = new JobConf(PageRankJob.getClass)
    conf.setJobName("PageRank")
//    conf.addResource("classpath:/hadoop/core-site.xml")
//    conf.addResource("classpath:/hadoop/hdfs-site.xml")
//    conf.addResource("classpath:/hadoop/mapred-site.xml")
    conf
  }
  def run(hdfConfig: HdfsPathConfig) = {
    val conf = config()
    val job = Job.getInstance(conf)

    val hdfs = new HdfsHelper(PageRankJob.HDFS, conf)
    hdfs.rmr(hdfConfig.input)
    hdfs.mkdirs(hdfConfig.input)
    hdfs.mkdirs(hdfConfig.inputPr)
    hdfs.copyFile(hdfConfig.page, hdfConfig.input)
    hdfs.copyFile(hdfConfig.pagerank, hdfConfig.inputPr)

    job.setJarByClass(PageRankJob.getClass)

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    job.setMapperClass(classOf[PageMapper])
    job.setReducerClass(classOf[PageReducer])

//    job.setInputFormatClass(classOf[TextInputFormat])
//    job.setOutputFormatClass(classOf[TextOutputFormat])

    FileInputFormat.setInputPaths(job, new Path(hdfConfig.input))
    FileOutputFormat.setOutputPath(job, new Path(hdfConfig.tmpMatrix))

    job.waitForCompletion(true)
  }
  def main(args: Array[String]) {
    val hdfs = if (System.getProperty("hdfs") != null) System.getProperty("hdfs") else ""
    Try {
      val path = HdfsPathConfig("res/pagerank/page.csv", "res/pagerank/pr.csv", hdfs + "/user/hduser/pagerank",
        hdfs + "/user/hduser/pagerank/pr", hdfs + "/user/hduser/pagerank/tmp1", hdfs + "/user/hduser/pagerank/tmp2",
        hdfs + "/user/hduser/pagerank/result")
      run(path)
      val iter = 3
      for (i <- 1 to iter) {
        // 迭代执行
        MatrixJob.run(path)
      }
      val hdfsHelper = new HdfsHelper(PageRankJob.HDFS, config())
      hdfsHelper.rename(path.tmpPr, path.result)

    } match {
      case NonFatal(e) => e.printStackTrace()
    }
  }
}
