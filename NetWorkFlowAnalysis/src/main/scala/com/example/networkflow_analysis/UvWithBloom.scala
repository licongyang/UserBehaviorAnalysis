package com.example.networkflow_analysis

import com.example.networkflow_analysis.UvWithBloom.Bloom
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis


object UvWithBloom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 指定事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 用相对路径定义数据源
    val resource = getClass.getResource("/UserBehavior.csv")
    println(resource.getPath)
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      // 指定事件时间字段
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      // 基于keyedstream
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom())

    dataStream.print("")

    env.execute("uv with bloom job")
  }

  // 自定义窗口触发器，每个元素来的触发,调用process
  class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      // 每来一条数据，就直接触发窗口操作，并清空所有窗口状态
      TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    // 一个元素来了，就触发去redis处理一次（不用关心关窗）
    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    // 没有定义定时器，pure清空数据，可以放到onelement处理中
    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
  }

  // 定义一个布隆过滤器
  // 一个窗口数据特别多，几亿（一条按几百byte，百亿byte（100byte,压缩到1位，800位压缩1位-》1000：1），就是几十g（1000：1-》几十mb）,redis也会充爆）、几十亿数据可以考虑布隆过滤器
  // 数据存不下，怎么办？
  // key的信息简化、压缩；不关心key内容，只关心有没有（0，1）
  // 要去重的一条数据（key,确定唯一找到的位-》hash,概率性告诉你存在不存在，跟hash函数有关，有可能hash碰撞），就变成一个位图（存储所有键是否存在）中的位
  // 这里可以传入参数，默认布隆过滤器大小；int类型有32位，正负20亿，位符存储空间接近2g(接近千亿数据)； 刚几十亿数据 大约存储空间几g，放到布隆过滤器也就几十mb(压缩比1000：1)；
  // long那存储的数据就可以认为没上限
  class Bloom(size: Long) extends Serializable {
    // 位图的总大小
    // 默认16mb（24 + 3 = 27）  1024是2的十次，2的4次是16， 字节8位 = 2的三次
    // 1 << 27 1后面有27个0
    private val cap = if (size > 0) size else 1 << 27

    // 定义hash函数
    // value（key）一个字符串，seed:一个随机种子； 返回long类型结果
    def hash(value: String, seed: Int): Long = {
      var result: Long = 0L
      // string一位一位判断什么，根据每位编码值叠加，得到结果
      for (i <- 0 until value.length) {
        result = result * seed + value.charAt(i)
      }
      // cap -1 : 前面是0，后面27个1
      result & (cap - 1)
    }

  }
}
// 基于window的processfunction
// 存入redis： 每个窗口的位图，count
// 每来一个数据，就要跟redis交互，并输出；操作太频繁（时间换空间）
// 如何优化：可以不用每个元素来就清空，隔几个元素才触发一次（内存得到利用）
class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UVCount, String, TimeWindow] {
  // 不应该每个数据过来都创建jedis,应该在外部定义连接
  lazy val jedis = new Jedis("localhost",6379)
  // 布隆过滤器 64mb 能处理64g数据
  lazy val bloom = new Bloom(1 << 29)
  // 其中element为当前窗口收集起来的所有数据，因为前面trigger每来一条就触发窗口处理，那element只有当前数据
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UVCount]): Unit = {
    // 位图的存储方式：key是windowEnd（每个窗口都要过滤，都单独位图表示）,value是bitmap
    val storekey = context.window.getEnd.toString
    // 把每个窗口的UV统计数，（这个可以放到状态内存，当trigger每个元素来都清空数据，要存可以存redis）
    // count也放入redis
    var count = 0L
    // processfunction先从redis取到，然后判断新来的元素要不要过滤掉，过滤掉count不变，没有过滤掉count+1，位图标志位置1
    //把每个窗口的uv count值也存入名位count的redis表，存放内容（windowEnd-> uvCount）
    if(jedis.hget("count", storekey) != null){
      count = jedis.hget("count", storekey).toLong
    }
    // 用布隆过滤器判断当前用户是否已经存在
    val userId = elements.last._2.toString
    // 用userid算hash值，就是在位图的偏移量
    // seed尽量传入质数（大点），分布散开
    val offset = bloom.hash(userId, 61)
    // 定义一个标志位，判断redis位图中有没有这一位(getbit取某一位)
    val isExist = jedis.getbit(storekey, offset)
    if(!isExist){
      // 如果不存在，位图对应位置1，count+=1
      jedis.setbit(storekey, offset, true)

      /**
       * 127.0.0.1:6379> hgetall count
       *  1) "1511661600000"
       *  2) "855"
       *  3) "1511665200000"
       *  4) "694"
       *  5) "1511668800000"
       *  6) "179"
       *  7) "1511672400000"
       *  8) "858"
       *  9) "1511679600000"
       * 10) "860"
       */
      jedis.hset("count", storekey, (count + 1).toString)
      out.collect(UVCount(storekey.toLong, count + 1))
    }else{
      // 如果存在,位图不变，count不变
      // 输出UVCount
      out.collect(UVCount(storekey.toLong, count))
    }


  }

}