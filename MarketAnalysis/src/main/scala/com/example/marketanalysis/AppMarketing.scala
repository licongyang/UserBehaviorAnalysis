package com.example.marketanalysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

object AppMarketing {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource( new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      // 按照渠道和行为统计分组
      .map(data => {
        ("dummyKey", 1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(10))
      // 要预聚合，定义aggregate, 来一个加1；后面再输出包装类
      .aggregate(new CountAgg(),  new MarketingCountTotal())
    // 标准10秒一个输出，开始输出数据count少，后面输出多，到一个小时之后，基本稳定（生成数据的速率稳定）
    dataStream.print()

    env.execute("app market  by channel")
  }

}

class CountAgg() extends AggregateFunction[(String, Long), Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: (String, Long), acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class MarketingCountTotal() extends WindowFunction[Long, MarketingViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp(window.getStart).toString
    val endTs = new Timestamp(window.getEnd).toString
    // 拿到前面预聚合的结果
    val count = input.iterator.next()

    out.collect( MarketingViewCount(startTs, endTs, "app marketing", "total", count))
  }
}


