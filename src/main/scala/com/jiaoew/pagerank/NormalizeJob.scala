package com.jiaoew.pagerank

import java.lang.Iterable

import com.jiaoew.pagerank.PageRankJob.HdfsPathConfig
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.JavaConversions._

/**
 * Created by jiaoew on 14/12/20.
 */
class NormalMapper extends Mapper[LongWritable, Text, Text, Text] {

  val k = new Text("1")

  override def map(key: LongWritable, values: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    println(values.toString())
    context.write(k, values)
  }
}

class NormalReducer extends Reducer[Text, Text, Text, Text] {
  override def reduce(key: Text, values: Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val sum = values.foldLeft(0.0f)((x, y)=> x + PageRankJob.DELIMITER.split(y.toString)(1).toFloat)

    for {
      line <- values
    } yield {
      val vals = PageRankJob.DELIMITER.split(line.toString());
      val k = new Text(vals(0))

      val f = vals(1).toFloat
      val v = new Text(PageRankJob.scaleFloat(f / sum))
      context.write(k, v)

      System.out.println(k + ":" + v)
    }

  }
}

object NormalizeJob {

  def run(config: HdfsPathConfig) = {
    val conf = PageRankJob.config()

    val hdfs = new HdfsHelper(PageRankJob.HDFS, conf)
    hdfs.rmr(config.result)

    val job = Job.getInstance(conf)
    job.setJarByClass(getClass)

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    job.setMapperClass(classOf[NormalMapper])
    job.setReducerClass(classOf[NormalReducer])

//    job.setInputFormatClass(classOf[TextInputFormat])
//    job.setOutputFormatClass(classOf[TextOutputFormat])

    FileInputFormat.setInputPaths(job, new Path(config.inputPr))
    FileOutputFormat.setOutputPath(job, new Path(config.result))

    job.waitForCompletion(true)
  }
}
