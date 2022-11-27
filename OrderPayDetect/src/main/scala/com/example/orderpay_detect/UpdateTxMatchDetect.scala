package com.example.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 *  两边流都会等等对方5s,
 *  来了事件数据，什么时候触发超时喃？
 *  依据水位线和定时器设置的时间参数，
 *  这里水位线是用的事件时间，来一条新的升序事件，就推动水位线；
 *  多条输入流，当前processstream的时钟按照小的来
 *  比如： 支付：0845 ，0850
 *        订单：049
 *  那process的水位线为049；
 *  要想触发0845，就要来个订单 0851,支付来个0851，让 水位线（需要多条流的最小值）推到到0851（> 0845 + 5）,小于这个值的数据默认已到达可处理
 *  需要来的事件时间超过，超过定时器设置的时间，才会触发
 *
 */
object UpdateTxMatchDetect {

  // 定义侧输出流tag
  // 一种订单有，但第三方支付没有
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  // 一种第三方支付有，但订单没有（大量请求，超时丢失）
  val unmatchReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取订单输入流
    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map(data =>{
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .filter(_.txId != "")
      // 因为前面过滤掉了create, 后面再指定水位线；那么只在pay事件上面设置了水位线
      // 这里来create事件，不会影响水位线推进
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 读取支付到账事件流
    val receiptEventStream = env.socketTextStream("localhost", 8888)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim.toLong)
      })
      // 根据数据清空，评估事件水位线字段及乱序延迟事件；
      // 这里因为数据有序，简化处理；但一般需要用乱序方式处理
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 将两条流连接起来，共同处理
    val processedStream = orderEventStream.connect(receiptEventStream)
      .process(new TxPayMatch())

    processedStream.print("matched")

    processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processedStream.getSideOutput(unmatchReceipts).print("unmatchedReceipt")

    env.execute("tx match job")
  }

  // 按照tx_id分组（分区），进来的数据都是同样的tx_id
  class TxPayMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
    // 单独处理，来第一条事件；通过保存状态，交互
    //  定义状态来保存已经到达的订单支付事件和到账事件
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent]))
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))

    // 订单支付事件数据的处理
    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 判断有没有对应的到账事件
      val receipt = receiptState.value()
      if(receipt != null){
        // 如果已经有receipt，在主流输出匹配信息
        out.collect((pay,receipt))
        // 这里来pay，那之前的payState应该是空的
        // 清空状态
        receiptState.clear()
      }else{
        // 如果还没到，那么把pay存入状态，并且注册一个定时器等待
        payState.update(pay)
        // 定时器触发的时间，需要看两边的数据清空，但不一定那边就另外一边早或者完；这里等待5s
        // 什么算没等来？
        // watermark可以处理延迟，需要timestampassigner指定水位线延迟处理时间；这里就可以通过水位线来处理乱序
        // 比如pay的事件时间为10：00， 那么这里是等水位线涨到10：05之后才触发定时器逻辑
        // 需要要求另外一条流的事件事件应该在10：05之前到达
        // 有多个上游任务，自己任务会维护多个分区watermark，取最小的分区watermark，作为自己的时钟
        // 两边connect后， 如果有一条流慢，会以慢的流的watermark为准
        /**
         * 查看数据清空，
         * pay:
         * 34729,pay,sd76f87d6,1558430844
         * receipt:
         * sd76f87d6,wechat,1558430847
         *
         * 发现receipt比pay晚三秒，如果这里设置2s，就匹配不上；这里能够匹配上
         */
        ctx.timerService().registerEventTimeTimer(pay.eventTime * 10000L + 5000L)
      }
    }
    // 到账事件的处理
    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 同样的处理流程
      val pay = payState.value()
      if(pay != null){
        out.collect((pay, receipt))
      }else{
        receiptState.update(receipt)
        // 如果认为这边就是晚来的，那么可以指定定时器的时间为事件时间；但两条流说不准，有快有慢，还是加个延迟
        ctx.timerService().registerEventTimeTimer(receipt.eventTime * 1000L + 5000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 到时间了，如果还没有收到某个事件，那么输出报警信息
      if(payState.value() != null){
        // 说明receipt没来
        ctx.output(unmatchedPays, payState.value())
      }
      if(receiptState.value() != null){
        ctx.output(unmatchReceipts, receiptState.value())
      }
      // 清空操作
      payState.clear()
      receiptState.clear()
    }
  }

}


