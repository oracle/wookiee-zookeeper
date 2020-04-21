/*
 * Copyright (c) 2020 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.webtrends.harness.component.zookeeper.discoverable

import akka.actor.Actor
import akka.pattern.ask
import akka.util.Timeout
import com.webtrends.harness.command.{CommandException, _}

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * @author Michael Cuthbert, Spencer Wood
 */
trait DiscoverableCommandExecution extends CommandHelper with Discoverable {
  this: Actor =>
  import context.dispatcher

  /**
   * Executes a discoverable command where ever it may be located
   */
  def executeDiscoverableCommand[Input <: Product : ClassTag, Output <: Any : ClassTag]
    (basePath: String, id: String, bean: Input)(implicit timeout: Timeout): Future[Output]= {

    initCommandManager flatMap { cm =>
      getInstance(basePath, id) flatMap { in =>
        (cm ? ExecuteRemoteCommand[Input](id, in.getAddress, in.getPort, bean, timeout)) (timeout).mapTo[Output]
      }
    } recover {
      case f: Throwable =>
        throw CommandException("CommandManager", f)
    }
  }

  /**
    * Executes a discoverable command on every server that is hosting it
    */
  def broadcastDiscoverableCommand[Input <: Product : ClassTag, Output <: Any : ClassTag]
    (basePath: String, id: String, bean: Input)(implicit timeout: Timeout): Future[Iterable[Output]]= {

    initCommandManager flatMap { cm =>
      getInstances(basePath, id) flatMap { in =>
        if (in.isEmpty)
          throw new IllegalStateException(s"No instances found for $basePath")

        val futures = in.map { i =>
          (cm ? ExecuteRemoteCommand[Input](id, i.getAddress, i.getPort, bean, timeout)) (timeout).mapTo[Output]
        }

        Future.sequence(futures)
      }
    } recover {
      case f: Throwable =>
        throw CommandException("CommandManager", f)
    }
  }
}
