package train.common

import mam.Dic
import mam.Utils.{printDf, udfLongToDateTime}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
 * 对 play数据的重新处理 划分 session 并对 session内的相同 video 时间间隔不超过30min的进行合并
 */

object PlaysProcessBySplitSession {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      //.master("local[4]")
      .appName("PlaysProcessBySplitSession")
      .getOrCreate()
    val playSchema = StructType(
      List(
        StructField(Dic.colUserId, StringType),
        StructField(Dic.colPlayEndTime, StringType),
        StructField(Dic.colVideoId, StringType),
        StructField(Dic.colBroadcastTime, FloatType)
      )
    )

    //val hdfsPath="hdfs:///pay_predict/"
    val hdfsPath = ""
    val playRawPath = hdfsPath + "data/train/common/raw/plays/behavior_*.txt"   //HDFS
    val playProcessedPath = hdfsPath + "data/train/common/processed/userpay/plays_new"  //userpay
    var play = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(playSchema)
      .csv(playRawPath)
    
    println("原始play数据大小 ",play.count())
    printDf("play",play)


    /**
     * 去重 删除单条播放数据不足30s或超过12h的播放记录
     */

    play = play.dropDuplicates()
    println("去除重复数据", play.count())  //7107619   spark 7107583
    printDf("play",play)
    play = play.filter(col(Dic.colBroadcastTime) > 30 and col(Dic.colBroadcastTime) < 43200)
    println("大于30s小于12h的播放数据", play.count())  //5684125  √
    printDf("play",play)

    /**
     * 删除不在medias中的播放数据
     */

    val mediasRawPath = hdfsPath+"data/train/common/raw/medias/medias.txt"  //train 多 HDFS路径
    val mediaSchema = StructType(
      List(
        StructField(Dic.colVideoId, StringType),
        StructField(Dic.colVideoTitle, StringType),
        StructField(Dic.colVideoOneLevelClassification, StringType),
        StructField(Dic.colVideoTwoLevelClassificationList, StringType),
        StructField(Dic.colVideoTagList, StringType),
        StructField(Dic.colDirectorList, StringType),
        StructField(Dic.colActorList, StringType),
        StructField(Dic.colCountry, StringType),
        StructField(Dic.colLanguage, StringType),
        StructField(Dic.colReleaseDate, StringType),
        StructField(Dic.colStorageTime, StringType),
        //视频时长
        StructField(Dic.colVideoTime, StringType),
        StructField(Dic.colScore, StringType),
        StructField(Dic.colIsPaid, StringType),
        StructField(Dic.colPackageId, StringType),
        StructField(Dic.colIsSingle, StringType),
        //是否片花
        StructField(Dic.colIsTrailers, StringType),
        StructField(Dic.colSupplier, StringType),
        StructField(Dic.colIntroduction, StringType)
      )
    )

    var medias = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(mediaSchema)
      .csv(mediasRawPath)

    medias = medias.dropDuplicates()
    println("meidas的去重后数据规模", medias.count()) //7889992  spark 7889894 7892391
    printDf("medias",medias)

    val mediasVideo = medias.select(Dic.colVideoId).distinct()
    println("medias中video数量", mediasVideo.count())  //7889991  spark 7892391

    play = play.join(mediasVideo, Seq(Dic.colVideoId), "inner")
    printDf("play",play)

    println("在medias的video中的play数据", play.count())  //5184090  spark 5192611

    /**
     * 去掉点击数据中的异常用户（存在多个featureCode的用户)
     */
      /////////////////////////////////////////别忘了修改！！！！！！！！！！！！
    val clickRawPath = hdfsPath+"data/train/common/raw/click/click_20200701.txt"
    val clickSchema = StructType(
      List(
        StructField(Dic.colSubscriberId, StringType),
        StructField(Dic.colDeviceMsg, StringType),
        StructField(Dic.colFeatureCode, StringType),
        StructField(Dic.colBigVersion, StringType),
        StructField(Dic.colProvince, StringType),
        StructField(Dic.colCity, StringType),
        StructField(Dic.colCityLevel, StringType),
        StructField(Dic.colAreaId, StringType),
        StructField(Dic.colItemType, StringType),
        StructField(Dic.colItemId, StringType),
        StructField(Dic.colPartitionDate, StringType)

      )
    )

    val click = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(clickSchema)
      .csv(clickRawPath)

    printDf("click",click)

    //println("click去重之后的数据规模", click.dropDuplicates().count())  //269894 spark 269894
    //选取用户和他所有的机型号
    val userFeatureCode = click.select(Dic.colSubscriberId, Dic.colFeatureCode).dropDuplicates()
    //统计用户机型号 并选取机型号唯一的用户
    val correctUser = userFeatureCode.groupBy(col(Dic.colSubscriberId)).agg(count(col(Dic.colFeatureCode)))
                                      .withColumnRenamed("count(featurecode)","count")
                                      .filter(col("count") === 1)
                                      .withColumnRenamed(Dic.colSubscriberId, Dic.colUserId)

    println("机型号唯一的用户", correctUser.count())  //114425 spark 114425

    play = play.join(correctUser, Seq(Dic.colUserId), joinType = "inner").drop("count")
    println("根据机型号处理后的play数据规模", play.count())  //1859905  spark 1862514
    printDf("play",play)

    /**
     * 计算开始时间 start_time
     */

    //end_time转换成long类型的时间戳    long类型 10位 单位 秒   colBroadcastTime是Int类型的 需要转化
    play = play.withColumn(Dic.colConvertTime, unix_timestamp(col(Dic.colPlayEndTime)))
    //计算开始时间并转化成时间格式
    play = play.withColumn(Dic.colPlayStartTime, udfLongToDateTime(col(Dic.colConvertTime) - col(Dic.colBroadcastTime).cast("Long")))
               .drop(Dic.colConvertTime)
    printDf("play",play)

    /**
     * 根据用户id和 video id划分部分，然后每部分按照start_time进行排序 上移获得 start_time_Lead_play 和 start_time_Lead_same_video
     * 并 选取start_time_Lead_play和start_time_Lead_same_play 在 end_time之后的数据
     */


    //获得同一用户下一条 same video play数据的start_time
    val win1 = Window.partitionBy(Dic.colUserId, Dic.colVideoId).orderBy(Dic.colPlayStartTime)
    //同一个用户下一个相同视频的开始时间
    play = play.withColumn(Dic.colStartTimeLeadSameVideo, lead(Dic.colPlayStartTime, 1).over(win1)) //下一个start_time
                .withColumn(Dic.colTimeGapLeadSameVideo, ((unix_timestamp(col(Dic.colStartTimeLeadSameVideo))) - unix_timestamp(col(Dic.colPlayEndTime))))
                .withColumn(Dic.colTimeGap30minSign, when(col(Dic.colTimeGapLeadSameVideo) < 1800, 0).otherwise(1))   //0和1不能反
                .withColumn(Dic.colTimeGap30minSignLag, lag(Dic.colTimeGap30minSign, 1).over(win1))
                //划分session
                .withColumn(Dic.colSessionSign, sum(Dic.colTimeGap30minSignLag).over(win1))
                //填充null 并选取 StartTimeLeadSameVideo 在 end_time之后的
                .na.fill(Map((Dic.colTimeGapLeadSameVideo, 0),(Dic.colSessionSign,0)))//填充空值
                .filter(col(Dic.colTimeGapLeadSameVideo) >= 0 ) //筛选正确时间间隔的数据

    printDf("play",play)
    /**
     * 合并session内相同video时间间隔在30min之内的播放时长
     */

    val timeSum = play.groupBy(Dic.colUserId,Dic.colVideoId,Dic.colSessionSign).agg(sum(col(Dic.colBroadcastTime)))
                      .withColumnRenamed("sum(broadcast_time)", Dic.colTimeSum)

    play = play.join(timeSum, Seq(Dic.colUserId, Dic.colVideoId, Dic.colSessionSign),"inner")
               .select(Dic.colUserId, Dic.colVideoId, Dic.colPlayStartTime, Dic.colBroadcastTime, Dic.colTimeGapLeadSameVideo, Dic.colSessionSign, Dic.colTimeSum)
    printDf("play",play)

    /**
     * 同一个session内相同video只保留第一条数据
     */

    val win2 = Window.partitionBy(Dic.colUserId, Dic.colVideoId, Dic.colSessionSign, Dic.colTimeSum).orderBy(Dic.colPlayStartTime)
    play = play.withColumn(Dic.colKeepSign, count(Dic.colSessionSign).over(win2))    //keep_sign为1的保留 其他全部去重
              .filter(col(Dic.colKeepSign)===1)
              .drop(col(Dic.colKeepSign))
              .drop(col(Dic.colBroadcastTime))
              .drop(col(Dic.colTimeGapLeadSameVideo))
              .drop(col(Dic.colSessionSign))
              .sort(Dic.colUserId, Dic.colPlayStartTime)


    printDf("play",play)

    println("plays处理结束, 数据规模 ", play.count())  //spark 907053


    play.write.mode(SaveMode.Overwrite).format("parquet").save(playProcessedPath)


  }
  }