package com.example.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderTimeoutWithoutCep {

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
    val timeoutWarningStream = orderEventStream.process(new OrderTimeoutWarning())

    timeoutWarningStream.print()

    env.execute("order timeout without cep job")
  }

}

// 实现自定义的处理函数
class OrderTimeoutWarning extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{
  // 保存pay是否来过的状态
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    // 先取出状态标识位
    val isPayed = isPayedState.value()

    // 只考虑一个create 一个pay
    if(value.eventType == "create" && !isPayed){
      // 如果遇到了create事件，并且pay没有来过，注册定时器开始等待
      // 设置事件事件之后十五分种触发定时器，
      // 问题是来了数据，必须等15分钟才能处理
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 15 * 60 * 1000L)
    }else if(value.eventType == "pay"){
      // 如果是pay事件，直接把状态改为true
      // 这里正常碰到pay,就表示支付成功了；
      // 这里考虑的是乱序场景，先来了pay,后来create；不管情况怎么样，都在定时器触发时处理
      isPayedState.update(true)
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    // 判断isPayed是否为true
    val isPayed = isPayedState.value()
    if(isPayed){
      out.collect(OrderResult(ctx.getCurrentKey, "order payed successfully"))
    }else{
      out.collect(OrderResult(ctx.getCurrentKey, "order timeout"))
    }

    //清空状态
    isPayedState.clear()
  }
}