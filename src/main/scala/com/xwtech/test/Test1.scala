package com.xwtech.test

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.{InvalidJobConfException, JobConf}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by gold on 2/28/18.
  */
object Test1 {
  Logger.getLogger("org").setLevel(Level.ERROR)
  class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any]() {
    override def generateFileNameForKeyValue(key: Any, value: Any, name: String) : String = {
      key.asInstanceOf[String]
    }

    override def checkOutputSpecs(ignored: FileSystem, job: JobConf): Unit = {
      val name: String = job.get(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR)
      var outDir: Path = if (name == null) null else new Path(name)
      //当输出任务不等于0 且输出的路径为空，则抛出异常
      if (outDir == null && job.getNumReduceTasks != 0) {
        throw new InvalidJobConfException("Output directory not set in JobConf.")
      }
      //当有输出任务和输出路径不为null时
      if (outDir != null) {
        val fs: FileSystem = outDir.getFileSystem(job)
        outDir = fs.makeQualified(outDir)
        outDir = new Path(job.getWorkingDirectory, outDir)
        job.set(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR, outDir.toString)
        TokenCache.obtainTokensForNamenodes(job.getCredentials, Array[Path](outDir), job)
        //下面的注释掉，就不会出现这个目录已经存在的提示了
        /* if (fs.exists(outDir)) {
             throw new FileAlreadyExistsException("Outputdirectory"
                     + outDir + "alreadyexists");
         }
      }*/
      }
    }
}
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test1")
    val sc = new SparkContext(conf)

    val input = args(0)
    val output = args(1)

    val origion = sc.textFile(input)
      .map(x => (x.split(",")(0),1))
      .saveAsHadoopFile(output,classOf[String], classOf[String],
        classOf[RDDMultipleTextOutputFormat])
  }
}
