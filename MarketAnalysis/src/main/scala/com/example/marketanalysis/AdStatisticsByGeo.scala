package com.example.marketanalysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
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
// 输出黑名单报警信息
case class BlackListWarnning(userId: Long, adId: Long, msg: String)
// 每5秒，输出最近一个小时的不同地域的广告点击量
object AdStatisticsByGeo {

  // 定义侧输出流的tag
  val blackListOutputTag: OutputTag[BlackListWarnning] = new OutputTag[BlackListWarnning]("blackList")

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

    //黑名单：记录一个用户对一个广告的点击量（状态），每日凌晨清空（定时器）
    // 自定义process function , 过滤大量刷点击的行为
    val filterBlackListStream = adEventStream
      // 不同的用户，不同的广告
//      .keyBy("userId", "adid") // key的类型为tuple
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUser(100))
      // 定义侧输出流，输出包装类样例类

    // 根据省份做分组，开窗聚合
    val adCountStream = filterBlackListStream
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



    adCountStream.print("count")
    filterBlackListStream.getSideOutput(blackListOutputTag).print("blacklist")

    env.execute("ad statistics job")
  }
  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]{
    // 定义状态， 保存当前用户对当前广告的点击量
    // 上面也按照用户和广告分组，这里只需要保存点击量
    lazy  val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
    // 保存是否发送过黑名单的状态
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("issent-state", classOf[Boolean]))
    // 保存定时器触发的时间戳
    lazy val resetTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))
    // 每条数据来了做什么操作
    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      // 取出count状态
      val curCount = countState.value()

      // 如果是第一次处理，注册定时器,每天00：00触发
      if(curCount == 0){
        // 当前系统时间=》 当前天数 =》 明天
        val ts = ( ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
        resetTime.update(ts)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }

      // 判断计数是否达到上限，如果达到则加入黑名单
      if(curCount >= maxCount){
        // 判断是否发送过黑名单， 只发送一次
        if(!isSentBlackList.value()){
          isSentBlackList.update(true)
          // 输出到侧输出流
          ctx.output(blackListOutputTag, BlackListWarnning(value.userId, value.adId, "click over " + maxCount + " times today"))
        }
        // 不输出主流
        return
      }

      // 计数状态加1， 输出数据到主流
      countState.update( curCount + 1)
      out.collect(value)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      // 定时器触发时，清空状态
      if(timestamp == resetTime.value()){
        isSentBlackList.clear()
        countState.clear()
        resetTime.clear()
      }
    }
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
