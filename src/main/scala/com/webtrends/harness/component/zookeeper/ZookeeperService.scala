/*
 * Copyright 2015 Webtrends (http://www.webtrends.com)
 *
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.webtrends.harness.component.zookeeper

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import com.webtrends.harness.logging.LoggingAdapter
import org.apache.zookeeper.CreateMode

import scala.collection.concurrent.TrieMap

object ZookeeperService extends LoggingAdapter {
  // Actor of type ZookeeperActor
  def getZkActor(implicit system: ActorSystem): Option[ActorRef] = mediatorMap.get(system)

  private val mediatorMap = TrieMap[ActorSystem, ActorRef]()

  private[harness] def getMediator(system: ActorSystem): ActorRef = {
    mediatorMap.get(system) match {
      case Some(zkActor) => zkActor
      case None => throw new IllegalStateException(s"No ZK Actor Registered for System: [$system]")
    }
  }

  private[harness] def registerMediator(actor: ActorRef)(implicit system: ActorSystem) = {
    log.info(s"Registering mediator: [${actor.path}], for actor system: [$system]")
    mediatorMap.put(system, actor)
  }

  private[harness] def unregisterMediator(system: ActorSystem) = {
    if (mediatorMap.contains(system)) {
      log.info(s"Unregistering mediator for actor system: [$system]")
      mediatorMap.remove(system) foreach(_ ! PoisonPill)
    }
  }

  @SerialVersionUID(1L) private[harness] case class SetPathData(path: String, data: Array[Byte],
                                                                create: Boolean = false, ephemeral: Boolean = false, namespace: Option[String] = None, async: Boolean = false)

  @SerialVersionUID(1L) private[harness] case class GetPathData(path: String, namespace: Option[String] = None)

  @SerialVersionUID(1L) private[harness] case class GetOrSetPathData(path: String, data: Array[Byte],
                                                                       ephemeral: Boolean = false, namespace: Option[String] = None)

  @SerialVersionUID(1L) private[harness] case class GetPathChildren(path: String, includeData: Boolean, namespace: Option[String] = None)

  @SerialVersionUID(1L) private[harness] case class GetRegistrationPath()

  @SerialVersionUID(1L) private[harness] case class CreateNode(path: String, createMode: CreateMode, data: Option[Array[Byte]], namespace: Option[String] = None)

  @SerialVersionUID(1L) private[harness] case class CreateCounter(path: String)

  @SerialVersionUID(1L) private[harness] case class GetNodeExists(path: String, namespace: Option[String] = None)

  @SerialVersionUID(1L) private[harness] case class DeleteNode(path: String, namespace: Option[String] = None)
}
