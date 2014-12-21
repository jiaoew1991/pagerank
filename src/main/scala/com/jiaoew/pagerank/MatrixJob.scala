package com.jiaoew.pagerank

import java.lang.Iterable

import com.jiaoew.pagerank.PageRankJob.HdfsPathConfig
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by jiaoew on 14/12/20.
 */

class MatrixMapper extends Mapper[LongWritable, Text, Text, Text] {

  var flag: String = ""

  override def setup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    flag = context.getInputSplit.asInstanceOf[FileSplit].getPath.getParent.getName
  }

  override def map(key: LongWritable, values: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    println(values.toString())
    val tokens = PageRankJob.DELIMITER.split(values.toString())

    flag match {
      case "tmp1" =>
        val row = values.toString().substring(0, 1)
        val vals = PageRankJob.DELIMITER.split(values.toString().substring(2)) // 矩阵转置
        vals.zipWithIndex.foreach { pair =>
          val k = new Text(s"${pair._2 + 1}")
          val v = new Text(s"A:$row,${pair._1}")
          context.write(k, v)
        }
      case "pr" =>
        for (i <- 1 to PageRankJob.PAGE_NUMS) {
          val k = new Text(s"$i")
          val v = new Text(s"B:${tokens(0)},${tokens(1)}")
          context.write(k, v)
        }
      case _ => println(s"error input")
    }
  }
}

class MatrixReducer extends Reducer[Text, Text, Text, Text] {
  override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val mapA = new mutable.HashMap[Int, Float]()
    val mapB = new mutable.HashMap[Int, Float]()
    var pr = 0f

    values.foreach { line =>
      println(line)
      val vals = line.toString
      val tokens = PageRankJob.DELIMITER.split(vals.substring(2))
      if (vals.startsWith("A:")) {
        mapA.put(tokens(0).toInt, tokens(1).toFloat)
      } else if (vals.startsWith("B:")) {
        mapB.put(tokens(0).toInt, tokens(1).toFloat)
      }
    }

    for {
      pair <- mapA
    } yield {
      val idx = pair._1
      pr += mapA(idx) * mapB(idx)
    }

    context.write(key, new Text(PageRankJob.scaleFloat(pr)))
    System.out.println(key + ":" + PageRankJob.scaleFloat(pr))
  }
}

object MatrixJob {

  def run(config: HdfsPathConfig) = {
    val conf = PageRankJob.config()

    val hdfs = new HdfsHelper(PageRankJob.HDFS, conf)
    hdfs.rmr(config.tmpPr)

    val job = Job.getInstance(conf)
    job.setJarByClass(getClass)

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    job.setMapperClass(classOf[MatrixMapper])
    job.setReducerClass(classOf[MatrixReducer])

    //    job.setInputFormatClass(classOf[TextInputFormat])
    //    job.setOutputFormatClass(classOf[TextOutputFormat])

    FileInputFormat.setInputPaths(job, new Path(config.tmpMatrix), new Path(config.inputPr))
    FileOutputFormat.setOutputPath(job, new Path(config.tmpPr))

    job.waitForCompletion(true)

    hdfs.rmr(config.inputPr)
    hdfs.rename(config.tmpPr, config.inputPr)
  }
}
