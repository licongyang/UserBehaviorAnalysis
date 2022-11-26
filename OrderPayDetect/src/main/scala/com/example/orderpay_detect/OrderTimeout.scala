package com.example.orderpay_detect

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.util

// 定义输入订单事件的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)
// 定义输出结果样例类
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1.读取订单数据
    // 从文件读取，结果正确，定时器触发的事件不准确，准确测试，需要读取流式输入
//    val resource = getClass.getResource("/OrderLog.csv")
//    val orderEventStream = env.readTextFile(resource.getPath)

      val orderEventStream = env.socketTextStream("localhost", 7777)

      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      // 数据是升序的
      .assignAscendingTimestamps(_.eventTime * 1000L)
      // 根据订单分组
      .keyBy(_.orderId)

    // 2.定义一个匹配模式
    val orderPayPattern = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 3. 把模式应用到stream，得到一个pattern stream
    val patternStream =CEP.pattern(orderEventStream, orderPayPattern)

    // 4. 调用select方法，提取事件序列，超时的事件要做报警提示
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

    /**
     * 输入：
     *  34729,create,,1558430842
     *  34730,create,,1558430843
     *  34729,pay,sd76f87d6,1558430844
     *  这时，并不会输出，因为水位线相对于时间戳是有延迟，需要根据水位线触发处理；需要再来个新的更大时间戳，才能推动水位线，触发处理
     *
     *  另外注意：如果前面已经触发超时的处理；再来乱序迟到的数据，因为该数据的时间窗口已关闭，所有迟到数据会丢弃
     */

    val resultStream = patternStream.select(orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect())

    resultStream.print("payed")

    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout job")

  }
}

// 自定义超时处理序列处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult]{
  // map value : 有开头begin，没结尾follow的事件
  // l是超时调用到这里的时间戳
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    // 当前超时事件id ,
    // 注意不要用follow个体模式获取订单id；虽然用订单id分组，开始和结束都应该包含同样的orderid，但这里是超时，可能没有follow
    val timeoutOrderId = map.get("begin").iterator().next().orderId.toLong
    OrderResult(timeoutOrderId, "timeout")
  }
}

// 自定义正常支付事件序列处理函数
class  OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = map.get("follow").iterator().next().orderId
    OrderResult(payedOrderId, "payed successfully")
  }
}
