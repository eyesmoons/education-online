package com.eyesmoons.qzpoint.streaming

import java.sql.ResultSet
import java.lang

import com.eyesmoons.qzpoint.utils.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RegisterStreaming {
    private val groupid = "register_group_test"

    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val conf: SparkConf = new SparkConf().setAppName("register_streaming_app").setMaster("local[*]")
            .set("spark.streaming.kafka.maxRatePerPartition", "100")
            .set("spark.streaming.stopGracefullyOnShutdown", "true")

        val ssc = new StreamingContext(conf, Seconds(3))
        val sparkContext: SparkContext = ssc.sparkContext
        sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
        sparkContext.hadoopConfiguration.set("dfs.nameservices", "mycluster")

        val topic = Array("register_topic")
        val kafkaMap: Map[String, Object] = Map[String, Object](
            "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop101:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupid,
            "auto.offset.reset" -> "earliest",
            "enable.auto.commit" -> "false"
        )

        ssc.checkpoint("hdfs://mycluster/sparkstreaming/checkpoint")

        //查询mysql中是否有偏移量
        val sqlProxy = new SqlProxy()
        val offsetMap = new mutable.HashMap[TopicPartition, Long]()
        val client = DataSourceUtil.getConnection

        try {
            sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?", Array(groupid), new QueryCallback {
                override def process(rs: ResultSet): Unit = {
                    while (rs.next()) {
                        val model = new TopicPartition(rs.getString(2), rs.getInt(3))
                        val offset = rs.getLong(4)
                        offsetMap.put(model, offset)
                    }
                    rs.close() //关闭游标
                }
            })
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            sqlProxy.shutdown(client)
        }

        //设置kafka消费数据的参数  判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
        val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
            KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topic, kafkaMap))
        } else {
            KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topic, kafkaMap, offsetMap))
        }

        val resultDStream: DStream[(String, Int)] = stream.filter(item => item.value().split("\t").length == 3)
            .mapPartitions(partitions => {
                partitions.map(item => {
                    val record: String = item.value()
                    val lineArr: Array[String] = record.split("\t")
                    val app_name: String = lineArr(1) match {
                        case "1" => "PC"
                        case "2" => "APP"
                        case _ => "Other"
                    }
                    (app_name, 1)
                })
            })

        //resultDStream.cache()
        resultDStream.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(60),Seconds(6))
        val updateFunc = (values: Seq[Int], state: Option[Int]) => {
            val currentCount = values.sum //本批次求和
            val previousCount = state.getOrElse(0) //历史数据
            Some(currentCount + previousCount)
        }
        resultDStream.updateStateByKey(updateFunc).print()

        //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
        stream.foreachRDD(rdd=>{
            val sqlProxy = new SqlProxy()
            val client = DataSourceUtil.getConnection
            try {
                val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                for (or <- offsetRanges) {
                    sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
                        Array(groupid, or.topic, or.partition.toString, or.untilOffset))
                }
            } catch {
                case e: Exception => e.printStackTrace()
            } finally {
                sqlProxy.shutdown(client)
            }
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
