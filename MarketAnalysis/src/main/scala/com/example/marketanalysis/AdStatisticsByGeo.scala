package com.example.marketanalysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

// 广告点击数据存在埋点日志中
// 输入的广告点击事件样例类
case class AdClickEvent( userId: Long, adId: Long, province: String, city: String, timestamp: Long)
// 按照省份统计的输出结果样例类 （哪个省的，那个时间窗口内的统计量）
case class CountByProvince(windowEnd: String, province: String, count: Long)

// 每5秒，输出最近一个小时的不同地域的广告点击量
object AdStatisticsByGeo {

  def main(args: Array[String]): Unit = {
    val env  = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取数据并转换为AdClickEvent
    // 同一个人在北京点了很多次广告；这种人是有风险，需要加入黑名单，权限限制，短信通知，当天限制
    val resoure = getClass.getResource("/AdClickLog.csv")
    val adEventStream = env.readTextFile(resoure.getPath)
      .map(data =>{
        val dataArray = data.split(",")
        AdClickEvent(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim,  dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)


    // 根据省份做分组，开窗聚合
    val adCountStream = adEventStream
      // 传入多个key（int/string）,就是tuple; 可以保成元组
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))

      /**
       *  aggregate 多态：
       *   def aggregate[ACC: TypeInformation, R: TypeInformation](
       *         aggregateFunction: AggregateFunction[T, ACC, R]): DataStream[R] = {
       *
       *   def aggregate[ACC: TypeInformation, V: TypeInformation, R: TypeInformation](
       *         preAggregator: AggregateFunction[T, ACC, V],
       *         windowFunction: WindowFunction[V, R, K, W]): DataStream[R] = {
       *
       *
       *   def aggregate[ACC: TypeInformation, V: TypeInformation, R: TypeInformation](
       *         preAggregator: AggregateFunction[T, ACC, V],
       *         windowFunction: ProcessWindowFunction[V, R, K, W]): DataStream[R] = {
       */
      // aggregate
      .aggregate(new AdCountAgg(), new AdCountResult())



    adCountStream.print()

    env.execute("ad statistics job")
  }

}

// 自定义预聚合函数
// aggregatefunction 是java interface
class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

// 自定义窗口函数
// windowfunction 类型是scala 特征trait ; 也可以用processwindowfunction
class AdCountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(new Timestamp(window.getEnd).toString, key, input.iterator.next()))
  }
}
