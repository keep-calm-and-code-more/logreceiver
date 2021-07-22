
/*
 * Copyright  2019 Blockchain Technology and Application Joint Lab, Linkel Technology Co., Ltd, Beijing, Fintech Research Center of ISCAS.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BA SIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package rep.sc.tpl

import org.json4s._
import org.json4s.jackson.JsonMethods._
import rep.app.conf.SystemProfile
import rep.protos.peer.ChaincodeId
import rep.utils.IdTool
import rep.sc.scalax.IContract

import rep.sc.scalax.ContractContext
import rep.sc.scalax.ContractException
import rep.protos.peer.ActionResult
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Connection
import com.rabbitmq.client.Channel

/**
 * 资产管理合约
 */

final case class Transfer(from: String, to: String, amount: Int)


class ContractAssetsTPL extends IContract {
  private val QUEUE_NAME = "shimlog"
  var factory:ConnectionFactory =  _
  var channel: Channel =  _
  var connection: Connection =   _
  // 需要跨合约读账户
  val chaincodeName = SystemProfile.getAccountChaincodeName
  val chaincodeVersion = SystemProfile.getAccountChaincodeVersion
  //val prefix = IdTool.getCid(ChaincodeId(chaincodeName, chaincodeVersion))

  implicit val formats = DefaultFormats

  def sendMessage(message:String): Unit = {
    try {
      channel.queueDeclare(QUEUE_NAME, false, false, false, null)
      channel.basicPublish("", QUEUE_NAME, null, message.getBytes)
      //println(message.slice(0, 25))
    } catch {
      case e: Exception => print("rabbitmq Exception")
    }
  }

  def init(ctx: ContractContext) {
    this.factory = new ConnectionFactory()
    this.factory.setHost("192.168.2.70")
    this.factory.setUsername("repchain")
    this.factory.setPassword("repchain")
    this.connection = factory.newConnection
    this.channel = connection.createChannel
    println(s"tid: ${ctx.t.id}")
  }

  def set(ctx: ContractContext, data: Map[String, Int]): ActionResult = {
    println(s"set data:$data")
    for ((k, v) <- data) {
      ctx.api.setVal(k, v)
      sendMessage(s"${ctx.t.id} ${ctx.api.pe.getSysTag} | ctx.api.setVal($k, $v)")
    }
    null
  }

  def transfer(ctx: ContractContext, data: Transfer): ActionResult = {
    if (!data.from.equals(ctx.t.getSignature.getCertId.creditCode))
      throw ContractException("只允许从本人账户转出")
    // 跨合约读账户，该处并未反序列化
    val stateEx = ctx.api.getStateEx(chaincodeName, data.to)
    sendMessage(s"${System.currentTimeMillis.toString} | ${ctx.t.id} | ${ctx.api.pe.getSysTag} | ctx.api.getStateEx($chaincodeName, ${data.to}) | $stateEx")
    if (stateEx == null)
      throw ContractException("目标账户不存在")
    val sfrom: Any = ctx.api.getVal(data.from)
    var dfrom = sfrom.asInstanceOf[Int]
    sendMessage(s"${System.currentTimeMillis.toString} | ${ctx.t.id} | ${ctx.api.pe.getSysTag} | ctx.api.getVal(${data.from}) | $dfrom")
    if (dfrom < data.amount)
      throw ContractException("余额不足")
    ctx.api.setVal(data.from, dfrom - data.amount)
    sendMessage(s"${System.currentTimeMillis.toString} | ${ctx.t.id} | ${ctx.api.pe.getSysTag} | ctx.api.setVal(${data.from}, $dfrom - ${data.amount}) | null")
    var dto = ctx.api.getVal(data.to).toString.toInt
    sendMessage(s"${System.currentTimeMillis.toString} | ${ctx.t.id} | ${ctx.api.pe.getSysTag} | ctx.api.getVal(${data.to}) | $dto")
    ctx.api.setVal(data.to, dto + data.amount)
    sendMessage(s"${System.currentTimeMillis.toString} | ${ctx.t.id} | ${ctx.api.pe.getSysTag} | ctx.api.setVal(${data.to}, $dto + ${data.amount}) | null")
    null
  }

  def put_proof(ctx: ContractContext, data: Map[String, Any]): ActionResult = {
    //先检查该hash是否已经存在,如果已存在,抛异常
    for ((k, v) <- data) {
      var pv0: Any = ctx.api.getVal(k)
      if (pv0 != null)
      //        throw new Exception("["+k+"]已存在，当前值["+pv0+"]");
        throw ContractException(s"$k 已存在，当前值为 $pv0")
      ctx.api.setVal(k, v)
      print("putProof:" + k + ":" + v)
    }
    null
  }

  /**
   * 根据action,找到对应的method，并将传入的json字符串parse为method需要的传入参数
   */
  def onAction(ctx: ContractContext, action: String, sdata: String): ActionResult = {
    val json = parse(sdata)
    action match {
      case "transfer" =>
        transfer(ctx, json.extract[Transfer])
      case "set" =>
        set(ctx, json.extract[Map[String, Int]])
      case "putProof" =>
        put_proof(ctx, json.extract[Map[String, Any]])
    }
  }

}
