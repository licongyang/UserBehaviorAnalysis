package com.example.marketanalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.util.Random

// 输入数据样例类
case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

// 输出结果样例类
case class MarketingViewCount( windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)
object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource( new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      // 按照渠道和行为统计分组
      .map(data => {
        ((data.channel, data.behavior), 1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(10))
      // 要预聚合，定义aggregate, 来一个加1；后面再输出包装类
      .process( new MarketingCountByChannel())

    dataStream.print()

    env.execute("app market  by channel")
  }


}

// 自定义数据源
// 如果需要定义生命周期，可以用richsourcefunction;简单的化，就用sourcefunction
class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior]{
  // 定义是否运行的标志位
  var running = true
  // 定义用户行为的集合 （可以增加权重）
  val behaviorTypes: Seq[String] = Seq("CLICK","DOWNLOAD","INSTALL", "UNINSTALL")
  // 定义渠道的集合
  val channelSets: Seq[String] = Seq("wechat", "weibo", "appstore", "huaweistore")
  // 定义一个随机数发生器
  val rand: Random = new Random()

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    // 定义一个生成数据的上限
    val maxElements = Long.MaxValue
    // 当前生成数据量
    var count = 0L

    // 随机生成所有数据
    while(running && count < maxElements){
      val id = UUID.randomUUID().toString
      // 随机数生成器的范围为集合大小
      val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSets(rand.nextInt(channelSets.size))
      val ts = System.currentTimeMillis()

      ctx.collect(MarketingUserBehavior(id, behavior, channel, ts))

      count += 1
      // 便于查看，10ms睡眠
      TimeUnit.MILLISECONDS.sleep(10L)
    }
  }

  override def cancel(): Unit = running = false
}

// 自定义处理函数
class MarketingCountByChannel() extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow]{
  // element已拿到所有数据
  // 也可以考虑去重
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp(context.window.getStart).toString
    val endTs = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2
    val count = elements.size

    out.collect( MarketingViewCount(startTs, endTs, channel, behavior, count))
  }
}