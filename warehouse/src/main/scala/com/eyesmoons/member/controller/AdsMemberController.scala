package com.eyesmoons.member.controller

import com.eyesmoons.member.service.AdsMemberService
import com.eyesmoons.util.HiveUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object AdsMemberController {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME","atguigu")
        val sparkSession: SparkSession = SparkSession.builder().appName("ads_member_controller").master("local[*]").enableHiveSupport().getOrCreate()
        val ssc: SparkContext = sparkSession.sparkContext

        ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
        ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
        HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
        AdsMemberService.queryDetailApi(sparkSession, "20200621")
    }
}
