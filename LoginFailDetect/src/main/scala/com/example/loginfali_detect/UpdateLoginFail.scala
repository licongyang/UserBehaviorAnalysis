package com.example.loginfali_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object UpdateLoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取事件数据
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map(data =>{
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })

    val warningStream = loginEventStream
      // 考虑每个人连续失败 （攻击： 同一个用户，一直换ip）
      .keyBy(_.userId)
      // 放大招process，只要当前用户的该组所有数据都来了，连续失败(次数为参数)就告警
      .process(new UpdateLoginWarning(2))

    warningStream.print()

    env.execute("login fail detect job")
  }
}
// 比较简单的场景，一些统计指标比较，datastream可以开窗聚合；table api ; sql；
// 复杂的需求，就需要用到process（状态编程）
class UpdateLoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning]{
  // 来一个登录失败，注册就放到list
  // 如果有登录成功，就清空
  // 定义状态，保存2秒内的所有登录失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))

  // 来一个数据，处理一次

  /**
   * 这个代码就写死比较两次的，如果比较三个
   * 就需要判断状态列表是不是有两个，并且当前的事件时间跟它们比较在2s就报警
   * 将最近两次失败状态保存，等待第三次
   *
   * 另外数据是乱序的，如果先来的事件时间很大，后来的失败事件时间小（迟到数据），那么就一直输出报警
   * 处理乱序数据比较麻烦，处理来了一个，又来一个，比较两个；当这两个并不是真正挨着的，可能中间有success
   * 思路： 有乱序，还是用watermark机制，还用定时器，按照watermark做延迟之后的一个时间去触发；
   * 可能之前该来的数据都按照顺序来了，再判断2s内是否连续失败
   *
   * 连续N次的失败喃？==》cep
   */
  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    if(value.eventType == "fail"){
      // 如果是失败，判断之前是否有登录失败事件
      val iter = loginFailState.get().iterator()
      if(iter.hasNext){
        // 如果已经有登录失败事件，就比较事件时间
        // 这里是两次比较，如果三次就是iter.next().next()比较
        val firstFail = iter.next()
        if(value.eventTime < firstFail.eventTime + 2){
          // 代表两次失败数据在两秒内，就输出报警
          out.collect(Warning(value.userId, firstFail.eventTime, value.eventTime, "login fail in 2 second."))
        }
        // 更新最近一次的登录失败事件，保存在状态里
//        loginFailState.update()
        // 因为只有一个数据，可以先清再加（当然也可以加多个）
        loginFailState.clear()
        loginFailState.add(value)
      }else{
        // 如果是第一次登录失败，直接添加到状态
        loginFailState.add(value)
      }
    }else{
      // 如果是成功，清空状态
      loginFailState.clear()
    }
  }


}
