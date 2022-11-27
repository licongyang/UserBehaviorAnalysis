package com.example.orderpay_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object TxMatchByJoin {

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

    // join处理
    val processedStream = orderEventStream.intervalJoin(receiptEventStream)
      .between(Time.seconds(-5), Time.seconds(5))
      // 还能输出侧输出流
      // join 只能把这种符合时间条件的数据包含在一起，
      // 但不能处理只有一个流，另一流没有，就不会处理丢弃
      .process(new TxPayMatchByJoin())

    processedStream.print()

    env.execute("tx pay match by join job")
  }

}
// 一般用于正常匹配的输出，
// 应用场景：传感器报警
// 温度流 & 烟雾流 匹配，才能做报警
// 这里处理的不匹配，其实不是很适合
class TxPayMatchByJoin() extends  ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left, right))
  }
}
