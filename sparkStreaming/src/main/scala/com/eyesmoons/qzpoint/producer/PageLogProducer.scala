package com.eyesmoons.qzpoint.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PageLogProducer {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("page_producer")

        val ssc = new SparkContext(sparkConf)
        val dstream: RDD[String] = ssc.textFile("hdfs://mycluster/sparkstreaming/logFile/page.log", 10)

        dstream.foreachPartition(partition=>{
            val props = new Properties()
            props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop101:9092")
            props.put("acks", "1")
            props.put("batch.size", "16384")
            props.put("linger.ms", "10")
            props.put("buffer.memory", "33554432")
            props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer")
            props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer")

            val producer = new KafkaProducer[String, String](props)
            partition.foreach(item=>{
                val msg = new ProducerRecord[String, String]("page_topic", item)
                producer.send(msg)
            })

            producer.flush()
            producer.close()
        })
    }
}