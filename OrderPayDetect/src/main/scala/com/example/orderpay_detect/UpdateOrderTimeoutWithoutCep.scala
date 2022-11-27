package com.example.orderpay_detect

import com.example.orderpay_detect.UpdateOrderTimeoutWithoutCep.orderTimeoutOutputTag
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

// 正常支付，就直接处理，放到主流；未15分钟处理的，就放到侧输出流
object UpdateOrderTimeoutWithoutCep {
  // 定义超时事件侧输出流标签
  val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderouttime")
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1.读取订单数据
    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      // 数据是升序的
      .assignAscendingTimestamps(_.eventTime * 1000L)
      // 根据订单分组
      .keyBy(_.orderId)

    // 定义process function进行超时检测
    val orderResultStream = orderEventStream.process(new OrderPayMatch())
    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout without cep job")
  }

}

class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{
  // 定义是否支付状态
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))
  // 需要知道创建是否来过、匹配后要删除定时器，需要保存定时器时间戳；==合并》定时器时间戳为0，也表示create没来
  // 保存定时器触发的时间戳为状态
  lazy val timestampState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state", classOf[Long]))

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    // 先读取状态
    val isPayed = isPayedState.value()
    val timerTs = timestampState.value()

    // 根据事件的类型进行分类判断，做不同的处理逻辑
    // 只考虑一个create 一个pay场景
    if(value.eventType == "create"){
      // 如果是create事件，接下来判断pay是否来过
      if(isPayed){
        // 1.1 如果已经payed，匹配成功，输出主流数据，清空状态
        // 这里不考虑乱序超时十五分钟以上的场景，15分钟之后的pay先来了，再来了create，，比较少，不考虑（要做需要再细化）
        // 延迟数据都要保存，可能数据量较大；并且延迟这么大，就不要考虑实时，直接用批处理就行了
        out.collect(OrderResult(value.orderId, "payed successfully"))
        ctx.timerService().deleteEventTimeTimer(timerTs)
        isPayedState.clear()
        timestampState.clear()
      }else{
        // 1.2 如果没有payed，注册定时器等待pay到来
        val ts = value.eventTime * 1000L + 15 * 60 * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        timestampState.update(ts)
      }
    }else if(value.eventType == "pay"){
      // 2. 如果是pay事件，那么判断是否created,用timer表示
      if(timerTs > 0){
        // 2.1 如果有定时器，说明已经有create来过
        // 需要判断两者时间戳，是否超时；
        if(timerTs > value.eventTime * 1000L){
          // 2.1.1 如果当前pay时间，还没到定时器事件，正常匹配，那么输出主流
          out.collect(OrderResult(value.orderId, "payed successfully"))
        }else{
          // 2.1.2 如果当前pay时间超过定时器时间，那么输出到侧输出流
          ctx.output(orderTimeoutOutputTag, OrderResult(value.orderId, "payed but already timeout"))
        }
        // 输出结束，清空状态
        ctx.timerService().deleteEventTimeTimer(timerTs)
        isPayedState.clear()
        timestampState.clear()
      }else{
        // 2.2 pay先到了
        // 有可能丢失create数据，也可能create乱序迟到，可以注册定时器等一下处理
        isPayedState.update(true)
        // 这里注册的eventtime, 当前watermark还没等到这个时间；
        // 只有watermark涨到value.eventTime才会触发定时器
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L)
        timestampState.update(value.eventTime * 1000L)

      }
    }
  }
  // 定时器触发场景：
  // 1. create来了，没等到pay
  // 2. pay来了。等creat没来
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    // 根据状态的值，判断哪个数据没来
    if(isPayedState.value()){
      // 如果为true， 表示pay先到了，没等到create
      ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "already payed but not found create log"))
    }else{
      // 表示create到了，没等到pay
      ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
    }
    // 清空状态
    isPayedState.clear()
    timestampState.clear()
  }
}