package com.eyesmoons.member.controller

import com.eyesmoons.member.service.DwsMemberService
import com.eyesmoons.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwsMemberController {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")

        val conf: SparkConf = new SparkConf().setAppName("dws_member_controller").setMaster("local[*]")
        //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        //      .registerKryoClasses(Array(classOf[DwsMember]))
        val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

        val ssc: SparkContext = sparkSession.sparkContext
        ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
        ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")

        HiveUtil.openCompression(sparkSession)
        HiveUtil.openDynamicPartition(sparkSession)

//        DwsMemberService.importMember(sparkSession, "20200621") //根据用户信息聚合用户表数据

        DwsMemberService.importMemberUseApi(sparkSession, "20190722")
    }
}
