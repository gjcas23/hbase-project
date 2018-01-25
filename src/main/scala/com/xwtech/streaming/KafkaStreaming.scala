package com.xwtech.streaming

import com.xwtech.util.{HBaseUtil, LocationConfig}
import org.apache.hadoop.hbase.client.{Connection, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by gold on 10/18/17.
  */
object KafkaStreaming {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val brokers=LocationConfig.kafkaBrokers
    val topics=LocationConfig.kafkaSourceTopic
    val groupid=LocationConfig.groupId
    val topicsSet: Set[String] =topics.split(",").toSet
    val chechpointpath = args(0)
    ssc.checkpoint(chechpointpath)
    val kafkaParams=Map[String,Object]("bootstrap.servers"->brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "false"
    )
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String,String](topicsSet,kafkaParams)
    )
    stream.foreachRDD(rdd => {
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(partitionRecords => {
        val conn: Connection = HBaseUtil.getConnection
        try {
          val hbaseTable: HTable = HBaseUtil.getHTable(LocationConfig.hTable, conn)
          hbaseTable.setAutoFlushTo(false)

          partitionRecords.foreach(line => {
            val Array(msisdn, imei, city, loc_city, province, start_time,
            start_ci, start_lon, start_lat, end_time, end_ci,
            end_lon, end_lat, duration, lac, grid) = line.value().split("\\|")
            val rowkey = msisdn + "_" + start_time
            val put = new Put(Bytes.toBytes(rowkey))
            put.addColumn(Bytes.toBytes("family"), Bytes.toBytes("column"), Bytes.toBytes(line.value()))
          })
          hbaseTable.flushCommits()
        } catch {
          case e => e.printStackTrace()
        } finally {
          HBaseUtil.closeConnect(conn)
        }
      })
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
