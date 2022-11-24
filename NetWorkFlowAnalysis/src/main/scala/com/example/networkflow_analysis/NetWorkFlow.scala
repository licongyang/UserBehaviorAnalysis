package com.example.networkflow_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer

// 输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

// 窗口聚合结果样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetWorkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val dataStream = env.readTextFile("/Users/alfie/workspace/code/learn/flink-lean/UserBehaviorAnalysis/NetWorkFlowAnalysis/src/main/resources/apache.log")
      .map(data => {
        val dataArray = data.split(" ")
        // 定义时间转换
        val simpleDataFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDataFormat.parse(dataArray(3).trim).getTime
        // 数据中- - userid，username忽略
        ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
      })
      // 数据的时间timestamp有乱序
      // 分析数据，看乱序清空，先确定一个延迟
      // 根据最大延迟，看未处理迟到数据多少；要还是很多，那就要调大延迟
      // 23.92.61.96 - - 18/05/2015:10:05:53 +0000 GET /
      // 53数据来了之后，01的数据才来
      // 74.125.40.19 - - 18/05/2015:10:05:01 +0000 GET /?flav=rss20
      // 大概推断，延迟在60s
      // 这里5秒就要统计，但延迟要50秒，延迟太高了
      // 如果实时性要求较高，不想这么高的延迟，那么可以这里先设置一个小延迟
      // 后面做迟到数据处理（窗口allowlazydata 额外处理；要没处理完，再sideout处理）
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        // flink 只认long,当成毫秒，这里timestamp时间就是毫秒
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })
      // 统计热门页面
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      // 允许60秒的迟到数据
      .allowedLateness(Time.seconds(60))
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))

    dataStream.print()
    env.execute()

  }

  // 自定义预聚合函数
  class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  // 自定义窗口处理函数
  class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
    }
  }

  // 自定义排序输出处理函数
  case class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
    lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount]))

    override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
      urlState.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 从状态中拿到数据
      val allUrlViews: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]()

      val iter = urlState.get().iterator()
      while (iter.hasNext) {
        allUrlViews += iter.next()
      }
      urlState.clear()
      //排序 根据count 从高到低
      val sortedUrlViews = allUrlViews.sortWith(_.count > _.count).take(topSize)

      //格式化结果输出
      val result: StringBuilder = new StringBuilder()
      result.append("时间： ").append(new Timestamp( timestamp - 1)).append("\n")
      for(i <- sortedUrlViews.indices){
        val  currentUrlViews = sortedUrlViews(i)
        result.append("No: ").append(i + 1).append(":")
          .append(" URL=").append(currentUrlViews.url)
          .append(" 访问量=").append(currentUrlViews.count).append("\n")
      }

      result.append("================================================")
      Thread.sleep(1000)
      out.collect(result.toString())

    }
  }
}
