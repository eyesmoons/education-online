package com.eyesmoons.sellcourse.controller

import com.eyesmoons.sellcourse.service.DwsSellCourseService
import com.eyesmoons.util.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwsSellCourseController {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME","atguigu")
        val sparkConf: SparkConf = new SparkConf().setAppName("sell_course_dws_controller").setMaster("local[*]")
            .set("spark.sql.shuffle.partitions", "15")
            .set("spark.sql.autoBroadcastJoinThreshold", "1")

        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        val ssc: SparkContext = sparkSession.sparkContext

        ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
        ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
        HiveUtil.openDynamicPartition(sparkSession)
        HiveUtil.openCompression(sparkSession)
        DwsSellCourseService.importSellCourseDetail(sparkSession, "20190722")
    }
}
