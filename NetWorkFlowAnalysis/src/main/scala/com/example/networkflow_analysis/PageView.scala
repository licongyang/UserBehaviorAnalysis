package com.example.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

// 先定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, catagoryId: Int, behavior: String, timestamp: Long)

object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 用相对路径定义数据源
    val resource = getClass.getResource("/UserBehavior.csv")
    println(resource.getPath)
    val dataStream = env.readTextFile(resource.getPath)
      .map(data =>{
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .map(data => ("pv",1))
      // pv是固定字段，不起作用，亚key
      .keyBy(_._1)
      // 不用keyby,就需要用windowall
      .timeWindow(Time.hours(1))
      // 用"pv" 分组, 数据值求和
      .sum(1)
    // 历史数据重放，这里处理时间很快，但是事件时间是每小时输出一次
    // 每个人点击多次也是包含在这里
    dataStream.print("pv count")

    env.execute("page view job")
  }

}
