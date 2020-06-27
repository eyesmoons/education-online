package com.eyesmoons.qz.controller

import com.eyesmoons.qz.service.AdsQzService
import com.eyesmoons.util.HiveUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object AdsController {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME","atguigu")
        val sparkSession: SparkSession = SparkSession.builder().appName("qz_ads_controller").master("local[*]").enableHiveSupport().getOrCreate()

        val ssc: SparkContext = sparkSession.sparkContext
        ssc.hadoopConfiguration.set("fs.defaultFS","hdfs://mycluster")
        ssc.hadoopConfiguration.set("dfs.nameservices","mycluster")

        HiveUtil.openCompression(sparkSession)
        HiveUtil.openCompression(sparkSession)
        HiveUtil.useSnappyCompression(sparkSession)

        val dt = "20190722"
        AdsQzService.getTarget(sparkSession, dt)
        AdsQzService.getTargetApi(sparkSession, dt)
    }
}