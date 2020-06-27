package com.eyesmoons.member.controller

import com.eyesmoons.member.service.EtlDataService
import com.eyesmoons.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwdMemberController {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val conf: SparkConf = new SparkConf().setAppName("dwd_member_controller").setMaster("local[*]")

        val sparkSession: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

        val ssc: SparkContext = sparkSession.sparkContext

        ssc.hadoopConfiguration.set("fs.defaultFS","hdfs://mycluster")
        ssc.hadoopConfiguration.set("dfs.nameservices","mycluster")

        HiveUtil.openDynamicPartition(sparkSession)
        HiveUtil.openCompression(sparkSession)

        //对用户原始数据进行数据清洗 存入dwd层表中
        EtlDataService.etlBaseAdLog(ssc, sparkSession) //导入基础广告表数据
        println("广告数据清洗完毕")
        EtlDataService.etlBaseWebSiteLog(ssc, sparkSession) //导入基础网站表数据
        println("网站数据清洗完毕")
        EtlDataService.etlMemberLog(ssc, sparkSession) //清洗用户数据
        println("用户数据清洗完毕")
        EtlDataService.etlMemberRegtypeLog(ssc, sparkSession) //清洗用户注册数据
        println("用户注册数据清洗完毕")
        EtlDataService.etlMemPayMoneyLog(ssc, sparkSession) //导入用户支付情况记录
        println("用户支付数据清洗完毕")
        EtlDataService.etlMemVipLevelLog(ssc, sparkSession) //导入vip基础数据
        println("vip数据清洗完毕")
    }
}
