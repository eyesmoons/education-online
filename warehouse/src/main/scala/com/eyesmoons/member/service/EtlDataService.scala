package com.eyesmoons.member.service

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 数据导入dwd层
 */
object EtlDataService {
    /**
     * 导入用户vip基础数据
     * @param ssc
     * @param sparkSession
     */
    def etlMemVipLevelLog(ssc: SparkContext, sparkSession: SparkSession): Unit = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/pcenterMemViplevel.log").filter{
            item=>{
                val obj: JSONObject = JSON.parseObject(item)
                obj.isInstanceOf[JSONObject]
            }
        }.mapPartitions{
            par=>{
                par.map{
                    item=>{
                        val jSONObject = JSON.parseObject(item)
                        val discountval = jSONObject.getString("discountval")
                        val end_time = jSONObject.getString("end_time")
                        val last_modify_time = jSONObject.getString("last_modify_time")
                        val max_free = jSONObject.getString("max_free")
                        val min_free = jSONObject.getString("min_free")
                        val next_level = jSONObject.getString("next_level")
                        val operator = jSONObject.getString("operator")
                        val start_time = jSONObject.getString("start_time")
                        val vip_id = jSONObject.getIntValue("vip_id")
                        val vip_level = jSONObject.getString("vip_level")
                        val dn = jSONObject.getString("dn")
                        (vip_id, vip_level, start_time, end_time, last_modify_time, max_free, min_free, next_level, operator, dn)
                    }
                }
            }
        }.toDF().write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_vip_level")
    }

    /**
     * 导入用户付款信息
     *
     * @param ssc
     * @param sparkSession
     */
    def etlMemPayMoneyLog(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/pcentermempaymoney.log").filter{
            item=>{
                val obj: JSONObject = JSON.parseObject(item)
                obj.isInstanceOf[JSONObject]
            }
        }.mapPartitions{
            par=>{
                par.map{
                    item=>{
                        val jSONObject = JSON.parseObject(item)
                        val paymoney = jSONObject.getString("paymoney")
                        val uid = jSONObject.getIntValue("uid")
                        val vip_id = jSONObject.getIntValue("vip_id")
                        val site_id = jSONObject.getIntValue("siteid")
                        val dt = jSONObject.getString("dt")
                        val dn = jSONObject.getString("dn")
                        (uid, paymoney, site_id, vip_id, dt, dn)
                    }
                }
            }
        }.toDF().write.mode(SaveMode.Append).insertInto("dwd.dwd_pcentermempaymoney")
    }

    /**
     * 导入用户注册信息
     *
     * @param ssc
     * @param sparkSession
     * @return
     */
    def etlMemberRegtypeLog(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/memberRegtype.log").filter{
            item=>{
                val obj: JSONObject = JSON.parseObject(item)
                obj.isInstanceOf[JSONObject]
            }
        }.mapPartitions{
            par=>{
                par.map{
                    item=>{
                        val jsonObject = JSON.parseObject(item)
                        val appkey = jsonObject.getString("appkey")
                        val appregurl = jsonObject.getString("appregurl")
                        val bdp_uuid = jsonObject.getString("bdp_uuid")
                        val createtime = jsonObject.getString("createtime")
                        val domain = jsonObject.getString("webA")
                        val isranreg = jsonObject.getString("isranreg")
                        val regsource = jsonObject.getString("regsource")
                        val regsourceName = regsource match {
                            case "1" => "PC"
                            case "2" => "Mobile"
                            case "3" => "App"
                            case "4" => "WeChat"
                            case _ => "other"
                        }
                        val uid = jsonObject.getIntValue("uid")
                        val websiteid = jsonObject.getIntValue("websiteid")
                        val dt = jsonObject.getString("dt")
                        val dn = jsonObject.getString("dn")
                        (uid, appkey, appregurl, bdp_uuid, createtime, domain, isranreg, regsource, regsourceName, websiteid, dt)
                    }
                }
            }
        }.toDF().write.mode(SaveMode.Append).insertInto("dwd.dwd_member_regtype")
    }

    /**
     * 导入用户表数据
     *
     * @param ssc
     * @param sparkSession
     */
    def etlMemberLog(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        ssc.textFile("/user/atguigu/ods/member.log").filter{
            item=>{
                val jsonObject: JSONObject = JSON.parseObject(item)
                jsonObject.isInstanceOf[JSONObject]
            }
        }.mapPartitions{
            par=>{
                par.map{
                    item=>{
                        val jsonObject = JSON.parseObject(item)
                        val ad_id = jsonObject.getIntValue("ad_id")
                        val birthday = jsonObject.getString("birthday")
                        val email = jsonObject.getString("email")
                        val fullname = jsonObject.getString("fullname").substring(0, 1) + "xx"
                        val iconurl = jsonObject.getString("iconurl")
                        val lastlogin = jsonObject.getString("lastlogin")
                        val mailaddr = jsonObject.getString("mailaddr")
                        val memberlevel = jsonObject.getString("memberlevel")
                        val password = "******"
                        val paymoney = jsonObject.getString("paymoney")
                        val phone = jsonObject.getString("phone")
                        val newphone = phone.substring(0, 3) + "*****" + phone.substring(7, 11)
                        val qq = jsonObject.getString("qq")
                        val register = jsonObject.getString("register")
                        val regupdatetime = jsonObject.getString("regupdatetime")
                        val uid = jsonObject.getIntValue("uid")
                        val unitname = jsonObject.getString("unitname")
                        val userip = jsonObject.getString("userip")
                        val zipcode = jsonObject.getString("zipcode")
                        val dt = jsonObject.getString("dt")
                        val dn = jsonObject.getString("dn")
                        (uid, ad_id, birthday, email, fullname, iconurl, lastlogin, mailaddr, memberlevel, password, paymoney, newphone, qq,
                            register, regupdatetime, unitname, userip, zipcode, dt, dn)
                    }
                }
            }
        }.toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_member")
    }

    /**
     * 导入网站表基础数据
     *
     * @param ssc
     * @param sparkSession
     */
    def etlBaseWebSiteLog(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        val resultRdd: RDD[(Int, String, String, Int, String, String, String)] = ssc.textFile("/user/atguigu/ods/baswewebsite.log").filter(item => {
            val obj = JSON.parseObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions {
            par => {
                par.map {
                    item => {
                        val jsonObject: JSONObject = JSON.parseObject(item)
                        val siteid: Int = jsonObject.getIntValue("siteid")
                        val sitename: String = jsonObject.getString("sitename")
                        val siteurl: String = jsonObject.getString("siteurl")
                        val delete: Int = jsonObject.getIntValue("delete")
                        val createtime: String = jsonObject.getString("createtime")
                        val creator: String = jsonObject.getString("creator")
                        val dn: String = jsonObject.getString("dn")
                        (siteid, sitename, siteurl, delete, createtime, creator, dn)
                    }
                }
            }
        }
        resultRdd.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_website")
    }

    /**
     * 导入广告表基础数据
     * @param ssc
     * @param sparkSession
     */
    def etlBaseAdLog(ssc: SparkContext, sparkSession: SparkSession) = {
        import sparkSession.implicits._
        val result: Unit = ssc.textFile("/user/atguigu/ods/baseadlog.log").filter(item => {
            val obj = JSON.parseObject(item)
            obj.isInstanceOf[JSONObject]
        }).mapPartitions {
            partition => {
                partition.map {
                    item => {
                        val jsonObject: JSONObject = JSON.parseObject(item)
                        val adid = jsonObject.getIntValue("adid")
                        val adname = jsonObject.getString("adname")
                        val dn = jsonObject.getString("dn")
                        (adid, adname, dn)
                    }
                }
            }
        }.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_ad")
    }
}