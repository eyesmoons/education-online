package com.eyesmoons.member.dao

import com.eyesmoons.member.bean.QueryResult
import org.apache.spark.sql.SparkSession

object DwsMemberDao {
    def queryIdlMemberData(sparkSession: SparkSession) = {
        sparkSession.sql("select uid,ad_id,memberlevel,register,appregurl,regsource,regsourcename,adname," +
            "siteid,sitename,vip_level,cast(paymoney as decimal(10,4)) as paymoney,dt,dn from dws.dws_member ")
    }

}
