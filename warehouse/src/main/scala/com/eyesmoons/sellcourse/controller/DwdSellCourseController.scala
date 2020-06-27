package com.eyesmoons.sellcourse.controller

import com.eyesmoons.sellcourse.service.DwdSellCourseService
import com.eyesmoons.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwdSellCourseController {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME","atguigu")
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sellcourse_dwd_controller")

        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        val ssc: SparkContext = sparkSession.sparkContext

        ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
        ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")

        HiveUtil.openDynamicPartition(sparkSession)
        HiveUtil.openCompression(sparkSession)

        DwdSellCourseService.importSaleCourseLog(ssc, sparkSession)
        DwdSellCourseService.importCoursePay(ssc, sparkSession)
        DwdSellCourseService.importCourseShoppingCart(ssc, sparkSession)
    }
}
