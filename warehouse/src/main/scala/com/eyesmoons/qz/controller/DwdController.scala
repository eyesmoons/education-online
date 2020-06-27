package com.eyesmoons.qz.controller

import com.eyesmoons.qz.service.EtlDataService
import com.eyesmoons.util.HiveUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DwdController {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME","atguigu")
        val sparkSession: SparkSession = SparkSession.builder().appName("qz_dwd_controller").master("local[*]").enableHiveSupport().getOrCreate()

        val ssc: SparkContext = sparkSession.sparkContext

        ssc.hadoopConfiguration.set("fs.defaultFS","hdfs://mycluster")
        ssc.hadoopConfiguration.set("dfs.nameservices","mycluster")

        HiveUtil.openDynamicPartition(sparkSession)
        HiveUtil.openCompression(sparkSession)

        EtlDataService.etlQzChapter(ssc, sparkSession)
        EtlDataService.etlQzChapterList(ssc, sparkSession)
        EtlDataService.etlQzPoint(ssc, sparkSession)
        EtlDataService.etlQzPointQuestion(ssc, sparkSession)
        EtlDataService.etlQzSiteCourse(ssc, sparkSession)
        EtlDataService.etlQzCourse(ssc, sparkSession)
        EtlDataService.etlQzCourseEdusubject(ssc, sparkSession)
        EtlDataService.etlQzWebsite(ssc, sparkSession)
        EtlDataService.etlQzMajor(ssc, sparkSession)
        EtlDataService.etlQzBusiness(ssc, sparkSession)
        EtlDataService.etlQzPaperView(ssc, sparkSession)
        EtlDataService.etlQzCenterPaper(ssc, sparkSession)
        EtlDataService.etlQzPaper(ssc, sparkSession)
        EtlDataService.etlQzCenter(ssc, sparkSession)
        EtlDataService.etlQzQuestion(ssc, sparkSession)
        EtlDataService.etlQzQuestionType(ssc, sparkSession)
        EtlDataService.etlQzMemberPaperQuestion(ssc, sparkSession)
    }
}
