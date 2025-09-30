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

package edu.uci.ics.texera.web.storage

import edu.uci.ics.amber.engine.common.Utils.withLock
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import io.reactivex.rxjava3.core.{Observable, Single}
import io.reactivex.rxjava3.disposables.Disposable
import io.reactivex.rxjava3.subjects.BehaviorSubject

import java.util
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable

class StateStore[T](defaultState: T) {

  private val stateSubject = BehaviorSubject.createDefault(defaultState)
  private val serializedSubject = stateSubject.toSerialized
  private implicit val lock: ReentrantLock = new ReentrantLock()
  private val diffHandlers = new mutable.ArrayBuffer[(T, T) => Iterable[TexeraWebSocketEvent]]
  private val diffSubject = serializedSubject
    .startWith(Single.just(defaultState))
    .buffer(2, 1)
    .filter(states => states.get(0) != states.get(1))
    .map[Iterable[TexeraWebSocketEvent]] { states: util.List[T] =>
      withLock {
        diffHandlers.flatMap(f => f(states.get(0), states.get(1)))
      }
    }

  def getState: T = stateSubject.getValue

  def updateState(func: T => T): Unit = {
    withLock {
      val newState = func(stateSubject.getValue)
      serializedSubject.onNext(newState)
    }
  }

  def registerDiffHandler(handler: (T, T) => Iterable[TexeraWebSocketEvent]): Disposable = {
    withLock {
      diffHandlers.append(handler)
    }
    Disposable.fromAction { () =>
      withLock {
        diffHandlers -= handler
      }
    }
  }

  def getWebsocketEventObservable: Observable[Iterable[TexeraWebSocketEvent]] =
    diffSubject.onTerminateDetach()

  def getStateObservable: Observable[T] = serializedSubject.onTerminateDetach()

}
