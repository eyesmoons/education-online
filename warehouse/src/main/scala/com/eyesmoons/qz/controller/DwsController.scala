package com.eyesmoons.qz.controller

import com.eyesmoons.qz.service.DwsQzService
import com.eyesmoons.util.HiveUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DwsController {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME","atguigu")
        val sparkSession: SparkSession = SparkSession.builder().master("local[*]").appName("qz_dws_controller").enableHiveSupport().getOrCreate()

        val ssc: SparkContext = sparkSession.sparkContext

        ssc.hadoopConfiguration.set("fs.defaultFS","hdfs://mycluster")
        ssc.hadoopConfiguration.set("dfs.nameservices","mycluster")

        HiveUtil.openCompression(sparkSession)
        HiveUtil.openCompression(sparkSession)
        HiveUtil.useSnappyCompression(sparkSession)

        val dt = "20190722"
        DwsQzService.saveDwsQzChapter(sparkSession, dt)
        DwsQzService.saveDwsQzCourse(sparkSession, dt)
        DwsQzService.saveDwsQzMajor(sparkSession, dt)
        DwsQzService.saveDwsQzPaper(sparkSession, dt)
        DwsQzService.saveDwsQzQuestionTpe(sparkSession, dt)
        DwsQzService.saveDwsUserPaperDetail(sparkSession, dt)
    }
}
