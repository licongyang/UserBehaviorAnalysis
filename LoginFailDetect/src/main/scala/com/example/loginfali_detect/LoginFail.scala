package com.example.loginfali_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

// 输入的登录事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)
// 输出的异常报警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {
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
      .process(new LoginWarning(2))

    warningStream.print()

    env.execute("login fail detect job")
  }
}
// 比较简单的场景，一些统计指标比较，datastream可以开窗聚合；table api ; sql；
// 复杂的需求，就需要用到process（状态编程）
class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning]{
  // 来一个登录失败，注册就放到list
  // 2秒内连续失败2次，来一个失败就注册定时器，来一个就赛一个，2秒就触发定时器，判断有几个；
  // 如果有登录成功，就清空
  // 定义状态，保存2秒内的所有登录失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))

  // 来一个数据，处理一次
  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    // 获取失败列表状态
    val loginFailList = loginFailState.get()
    // 判断类型是否是fail， 只添加fail的事件到状态
    // 成功会影响处理，前面不能过滤掉成功的
    if(value.eventType == "fail"){
      // 判断是否第一次失败，会注册定时器
      if(!loginFailList.iterator().hasNext){
        // 定时器参数为毫秒
        // 问题：2秒之后触发，这个期间接受到success,那么前面的fail就清空了
        // 好的办法： 不用2秒才触发，达到次数就触发
        ctx.timerService().registerEventTimeTimer(value.eventTime  * 1000L + 2000L)
      }
      loginFailState.add(value)
    }else{
      // 如果是成功，清空状态
      loginFailState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    // 触发定时器的时候，根据状态里的失败个数决定是否输出报警
    val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
    val iter = loginFailState.get().iterator()
    while(iter.hasNext){
      allLoginFails += iter.next()
    }

    // 判断个数
    if(allLoginFails.length >= maxFailTimes){
      out.collect(Warning(allLoginFails.head.userId, allLoginFails.head.eventTime, allLoginFails.last.eventTime, "login fail in 2 seconds for " + allLoginFails.length ))
//      out.collect(Warning(ctx.getCurrentKey, ))
    }
    // 清空状态
    loginFailState.clear()
  }
}
