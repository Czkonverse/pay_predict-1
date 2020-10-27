package train.userpay


import org.apache.spark.sql.functions._

import mam.Dic
import mam.Utils.{calDate, printDf, udfAddOrderStatus}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, isnull, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}

object UserSplit {


  def main(args:Array[String]): Unit ={
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("UserSplit")
      .master("local[6]")
      .getOrCreate()

    //HDFS
    val hdfsPath="hdfs:///pay_predict/"
    val playsProcessedPath = hdfsPath + "data/train/common/processed/userpay/plays_new"
    val ordersProcessedPath = hdfsPath + "data/train/common/processed/userpay/orders"

//    //local
//    val hdfsPath=""
//    val playsProcessedPath = hdfsPath + "data/train/common/processed/plays"
//    val ordersProcessedPath = hdfsPath + "data/train/common/processed/orders"

    //老用户名单保存路径
    val oldUserSavePath = hdfsPath + "data/train/usersplit/"
    //新用户名单保存路径
    val newUserSavePath = hdfsPath + "data/train/usersplit/"

    val trainTime = args(0)+" "+args(1)
    val timeWindow = 30

    val play = spark.read.format("parquet").load(playsProcessedPath)
    //所有用户id的列表
    val allUsersList = play.select(col(Dic.colUserId)).distinct().collect().map(_(0)).toList
    //所有用户id的dataframe
    println("用户总人数：" + allUsersList.length)
    val allUsersDataFrame = play.select(col(Dic.colUserId)).distinct()


    val orderAll = spark.read.format("parquet").load(ordersProcessedPath)
    // 选择套餐订单
    val orderPackage = orderAll
      .filter(
      col(Dic.colResourceType).>(0)
      && col(Dic.colResourceType).<(4)
        //&&((col(Dic.colResourceId) === 100201)|| (col(Dic.colResourceId) === 101101)
        //||(col(Dic.colResourceId) === 101103)||(col(Dic.colResourceId) === 100701))  //100201,101101,101103,100701
    )

    /**
     * 正样本   order中在train_time后14天内的支付成功订单
     */
    val trainTimePost14 = calDate(trainTime, days = 14)
    var trainPos = orderPackage
      .filter(
      col(Dic.colOrderStatus).>(1)
      && col(Dic.colCreationTime).>=(trainTime)
      && col(Dic.colCreationTime).<(trainTimePost14)
    )

    /**
     * 老用户 在time-time_window到time时间段内成功支付过订单 或者 在time之前创建的订单到time时仍旧有效
     */

    val trainTimePre = calDate(trainTime,days = -timeWindow)
    var trainOrderOld = orderPackage
      .filter(
        col(Dic.colOrderStatus).>(1)
        && ((col(Dic.colCreationTime).>(trainTimePre) && col(Dic.colCreationTime).<(trainTime))
          || (col(Dic.colOrderEndTime).>(trainTime) && col(Dic.colCreationTime).<(trainTime)) )
      )

    val joinKeysUserId = Seq(Dic.colUserId)
    trainOrderOld = allUsersDataFrame.join(trainOrderOld,joinKeysUserId,"inner")

    printDf("trainOrderOld", trainOrderOld)

    val trainOld = trainOrderOld.select(col(Dic.colUserId)).distinct()

    var trainOldDataFrame = trainOrderOld.select(col(Dic.colUserId)).distinct()

    println("老用户的数量：" + trainOldDataFrame.count())

    //所有的订单状态都为1
    val trainPosWithLabel = trainPos.select(col(Dic.colUserId)).distinct().withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId)))
    println("正样本用户的数量：" + trainPosWithLabel.count())
    trainOldDataFrame = trainOldDataFrame.join(trainPosWithLabel,joinKeysUserId,"left")
    trainOldDataFrame = trainOldDataFrame.na.fill(0)

    var trainOldPos = trainOldDataFrame.filter(col(Dic.colOrderStatus).===(1))
    var trainOldNeg = trainOldDataFrame.filter(col(Dic.colOrderStatus).===(0))

    println("老用户正样本数量："+trainOldPos.count())
    println("老用户负样本数量："+trainOldNeg.count())

    // 如何负样本数量超过6倍正样本，抽样6倍
    if(trainOldNeg.count() > trainOldPos.count()*6) {
      trainOldNeg = trainOldNeg.sample(1.0).limit((trainOldPos.count()*6).toInt)
    }
    val trainOldResult = trainOldPos.union(trainOldNeg)

    println("老用户数据集生成完成！")
    trainOldResult.write.mode(SaveMode.Overwrite).format("parquet").save(oldUserSavePath+"trainusersold"+args(0))

    /**
     * 新用户 order中在train_time时间段支付套餐订单且不是老用户的用户为新用户的正样本，其余非老用户为负样本
     */

    var trainPosUsers = orderPackage
      .filter(
        col(Dic.colCreationTime).>=(trainTime)
          && col(Dic.colCreationTime).<(trainTimePost14)
          && col(Dic.colOrderStatus).>(1)
      ).select(col(Dic.colUserId)).distinct()
    trainPosUsers = trainPosUsers.except(trainOld)

    val trainNewPos = trainPosUsers.withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId)))

    var trainNegOrderUsers=orderPackage
      .filter(
        col(Dic.colCreationTime).>=(trainTime)
          && col(Dic.colCreationTime).<(trainTimePost14)
          && col(Dic.colOrderStatus).<=(1)
      ).select(col(Dic.colUserId)).distinct()

    trainNegOrderUsers = trainNegOrderUsers.except(trainOld).except(trainPosUsers)

    var trainPlay= play
          .filter(
            substring(col(Dic.colPlayEndTime),1,10) === args(0)   //当天有播放数据的用户
            && col(Dic.colBroadcastTime)>120
          ).select(col(Dic.colUserId)).distinct()

    trainPlay = trainPlay.except(trainOld).except(trainPosUsers).except(trainNegOrderUsers)

    if(trainPlay.count()>(9*trainPosUsers.count()-trainNegOrderUsers.count())){
            trainPlay=trainPlay.sample(1).limit((9*trainPosUsers.count()-trainNegOrderUsers.count()).toInt)
          }
    val trainNewNeg = trainPlay.union(trainNegOrderUsers).withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId))-1)

    val trainNewResult = trainNewPos.union(trainNewNeg)
    println("新用户正样本数量："+trainNewPos.count())
    println("新用户负样本数量："+trainNewNeg.count())


    println("新用户数据集生成完成！")
    trainNewResult.write.mode(SaveMode.Overwrite).format("parquet").save(newUserSavePath + "trainusersnew"+args(0))



  }

}
