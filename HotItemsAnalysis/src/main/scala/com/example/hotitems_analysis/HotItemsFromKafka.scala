package com.example.hotitems_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.util.Properties
import scala.collection.mutable.ListBuffer

// 先定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, catagoryId: Int, behavior: String, timestamp: Long)

// 定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

/**
 * 每隔 5 分钟输出最近一小时内点击量最多的前 N 个商品
 * 可以拆解成：
 *    1. 抽取出业务时间戳，告诉 Flink 框架基于业务时间做窗口
 *    2. 过滤出点击行为数据
 *    3. 按一小时的窗口大小，每 5 分钟统计一次，做滑动窗口聚合（Sliding Window）
 *    4. 按每个窗口聚合，输出每个窗口中点击量前 N 名的商品
 * 输出大屏，实时监控热门商品
 */
object HotItemsFromKafka {

  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    //序列化
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // 没配checkpoint,偏移量重置，选latest
//        properties.setProperty("auto.offset.reset", "earliest")
    properties.setProperty("auto.offset.reset", "latest")

    // 1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 防止输出出现乱序，可以把并行度设置为1,不影响结果正确
    env.setParallelism(1)
    // 时间语义，默认处理时间，修改为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. 读取数据


    // 这里flinkkafkaconsumer后面没有010 011是因为引用的kafka依赖 kafka_2.11没这个
    // kafka 通用连接器，flink1.7+支持，自动跟踪kafka最新客户端版本，更加好用，但可能不稳定；后面生产用这个
    // kafka序列化成String
    // 好处：可以一条条数据，看什么时候输出窗口数据
    // 这个是从文本文件作为数据源，本地测试用
    //    val dataStream = env.readTextFile("/Users/alfie/workspace/code/learn/flink-lean/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv")
    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("hotItems2", new SimpleStringSchema(), properties))
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      // 指定事件事件字段
      // Watermark 是用来追踪业务事件的概念，可以理解成 EventTime 世界中的时钟，用来指示当前处理到什么时刻的数据了
      // 因为数据事件是升序，可以直接用ascendingtimestamps,每条数据的业务时间就当做 Watermark
      // 就得到了一个带有时间标记的数据流了，后面就能做一些窗口的操作
      .assignAscendingTimestamps(_.timestamp * 1000)

    // 3. transform 处理数据
    // 只考虑pv操作
    val processedStream = dataStream
      // 过滤
      .filter(_.behavior == "pv")
      // 按照id分组
      /**
       * 参数是名称，那么返回是KeyedStream[T, JavaTuple],
       * def keyBy(firstField: String, otherFields: String*): KeyedStream[T, JavaTuple] =
       * 其中T为keyedstream 数据类型；javatuple为key的类型
       * 后面windowfunction期待的key类型就是Tuple类型
       */
      //      .keyBy("itemId")
      /**
       * 定义keyselector,K就是对应实际数据的类型long
       * def keyBy[K: TypeInformation](fun: KeySelector[T, K]): KeyedStream[T, K] = {
       */
      .keyBy(_.itemId)
      // 开窗
      .timeWindow(Time.hours(1), Time.minutes(5))
      // 聚合
      // sum针对某个字段求和；这里是用aggregate 预聚合并输出一个特殊数据结构（带windowend）
      /**
       * def aggregate[ACC: TypeInformation, V: TypeInformation, R: TypeInformation](
       * preAggregator: AggregateFunction[T, ACC, V],
       * windowFunction: WindowFunction[V, R, K, W]): DataStream[R] = {
       *
       */
      // 当keyby的key类型为Tuple用，WindowResult2
      // 当keyby的key类型为Long，WindowResult
      // .aggregate(AggregateFunction af, WindowFunction wf) 做基于窗口增量的聚合操作
      // 使用 AggregateFunction 提 前 聚 合 掉 数 据 ， 减 少 state 的 存 储 压 力
      // WindowFunction 将每个 key 每个窗口聚合后的结果带上其他信息进行输出：将 < 主键商品 ID ，窗口，点击 量 > 封装成了ItemViewCount 进行输出
      .aggregate(new Countagg(), new WindowResult())
      // 按照窗口分组
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))

    // 4. sink: 控制台输出
    //    dataStream.print("data stream")
    processedStream.print()
    env.execute("hot items job")

  }

  // 自定义预聚合函数
  // ACC累加器，状态类型
  // 这里的输出结果，就是后面窗口函数的输入

  class Countagg() extends AggregateFunction[UserBehavior, Long, Long] {

    // 初始累加器值
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    // 输出结果，就是累加器的值
    override def getResult(acc: Long): Long = acc

    // 重分区，两个累加器如何合并处理
    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  // 自定义预聚合函数计算平均值 累加和/数量 （Long 累加和, Int 数量）
  // 上面时间戳求平均数
  // 基本统计指标，就这样（除了排序）
  class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
    override def createAccumulator(): (Long, Int) = (0L, 0)

    override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1 + in.timestamp, acc._2 + 1)

    override def getResult(acc: (Long, Int)): Double = acc._1 / acc._2

    // 两个累加器如何合并
    override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1 + acc1._1, acc._2 + acc1._2)
  }

  // 自定义窗口函数，输出ItemViewCount w<: Window(是上界)
  // 对应 .keyBy(_.itemId)，这里windowfunction的key就是实际类型long
  class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
    override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
    }
  }

  // 对应 .keyBy("itemId")，这里窗口函数的key类型就是Tuple
  class WindowResult2() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      // count可以从input中获取，windowend可以从window获取，key可以从key:Tuple获取
      // tuple如何获取商品id（它是long类型），tuple接口类，需要先明确它的实例 Tuple 0 1 。。。;
      // 注意：有可能引用scala的Tuple1，所以引用：import org.apache.flink.api.java.tuple.{Tuple,Tuple1}
      /**
       * public class Tuple1<T0> extends Tuple {
       * public T0 f0;
       *
       * Tuple1的类型为T0(这里是商品id，Long类型),它的值是f0
       */
      val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0

      out.collect(ItemViewCount(itemId, window.getEnd, input.iterator.next()))
    }
  }

  // 自定义的处理函数
  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
    // 来了每条数据做什么
    // 来了数据，就保存状态；设置定时器，触发操作
    // 状态列表 默认控制
    private var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 命名状态变量的名字和状态变量的类型
      itemState = getRuntimeContext.getListState(
        new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
    }

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
      // 把每条数据存入状态列表
      itemState.add(value)
      //注册一个定时器 ,延迟1毫秒，当程序看到windowend+1的水位线watermark时，触发onTimer回调函数
      // watermark的进度是全局的，在processelement方法中，每当收到一条数据itemviewcount,就注册一个windowend+1定时器（flink框架会自动忽略同一时间的重复注册）
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    // 利用 timer 来判断何时收齐了某个 window 下所有商品的点击量数据
    // 定时器触发时，对所有数据排序，并输出结果
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 将所有state中的数据取出，放到一个list buffer中
      val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
      // for each 遍历，需要引入转换
      // 在Scala中需要调用Java的代码时，报错 value foreach is not a member of java.util.List
      // 原因是采用了Java的方式，没有进行转换为Scala的方式。
      import scala.collection.JavaConverters._
      for (item <- itemState.get().asScala) {
        allItems += item
      }

      // 按照count大小排序
      // sortby默认升序，需要降序:Ordering.Long.reverse
      val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse)
      // 取前N个
      val topNHotItems = sortedItems.take(topSize)

      // 清空状态（可以等数据输出之后，再清空）
      itemState.clear()

      // 将排名结果格式化输出
      val result: StringBuilder = new StringBuilder()
      // 现在定时器触发时间timestamp，当时窗口关闭时间+1，那么窗口关闭时间= timestamp -1
      result.append("time: ").append(new Timestamp(timestamp - 1)).append("\n")
      // 输出每一个商品的信息
      //      for(item <- 0 to topNHotItems.length -1)
      for (i <- topNHotItems.indices) {
        val currentItem = topNHotItems(i)
        result.append("No").append(i + 1).append(":")
          .append(" 商品ID=").append(currentItem.itemId)
          .append(" 浏览量=").append(currentItem.count)
          .append("\n")
      }

      result.append("=======================")
      // 控制输出频率.休眠1秒
      Thread.sleep(1000)

      out.collect(result.toString())


    }
  }
}
