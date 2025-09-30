/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.uci.ics.amber.engine.architecture.control.utils

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands._
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns._

trait PingPongHandler {
  this: TesterAsyncRPCHandlerInitializer =>

  override def sendPing(ping: Ping, ctx: AsyncRPCContext): Future[IntResponse] = {
    println(s"${ping.i} ping")
    if (ping.i < ping.end) {
      getProxy.sendPong(Pong(ping.i + 1, ping.end, myID), ping.to).map { ret: IntResponse =>
        println(s"${ping.i} ping replied with value ${ret.value}!")
        ret
      }
    } else {
      Future(ping.i)
    }
  }

  override def sendPong(pong: Pong, ctx: AsyncRPCContext): Future[IntResponse] = {
    println(s"${pong.i} pong")
    if (pong.i < pong.end) {
      getProxy.sendPing(Ping(pong.i + 1, pong.end, myID), pong.to).map { ret: IntResponse =>
        println(s"${pong.i} pong replied with value ${ret.value}!")
        ret
      }
    } else {
      Future(pong.i)
    }
  }

}
