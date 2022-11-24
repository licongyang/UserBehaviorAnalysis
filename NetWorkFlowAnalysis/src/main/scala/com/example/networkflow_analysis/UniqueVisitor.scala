package com.example.networkflow_analysis

import com.example.networkflow_analysis.PageView.getClass
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 输出对象样例类
case class UVCount(windowEnd: Long, uvCount: Long)
object UniqueVisitor {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 指定事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 用相对路径定义数据源
    val resource = getClass.getResource("/UserBehavior.csv")
    println(resource.getPath)
    val dataStream = env.readTextFile(resource.getPath)
      .map(data =>{
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      // 指定事件时间字段
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      // 时间窗口内去重
      .timeWindowAll(Time.hours(1))
      // 可以窗口函数，也可以先预聚合，在窗口函数
      .apply(new UVCountByWindoor())

    dataStream.print("pv count")

    env.execute("uv job")
  }

  class UVCountByWindoor() extends AllWindowFunction[UserBehavior, UVCount, TimeWindow]{
    override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UVCount]): Unit = {
      // 定义一个scala set，用于保存所有的数据（userid）并去重
      // 内存充足，可以放到set去重
      // 内存不足，可以放到redis去重
      // 一个窗口数据特别多，几亿、几十亿数据可以考虑布隆过滤器
      var idSet = Set[Long]()
      //把当前窗口所有数据的ID收集到set中，最后输出set大小
      for(userBehavior <- input){
        idSet += userBehavior.userId
      }
      // 输出uvcount
      out.collect(UVCount(window.getEnd, idSet.size))
    }
  }
}
