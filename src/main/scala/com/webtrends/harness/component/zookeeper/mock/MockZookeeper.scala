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

package com.webtrends.harness.component.zookeeper.mock

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.util.Timeout
import com.typesafe.config.Config
import com.webtrends.harness.component.zookeeper.config.ZookeeperSettings
import com.webtrends.harness.component.zookeeper.{ZookeeperActor, ZookeeperService}
import com.webtrends.harness.utils.ActorWaitHelper

import scala.concurrent.duration._

/**
  * Use this actor to spin up a local zookeeper for unit testing.
  * Requires org.apache.curator:curator-test
  * To use ZK service from a test class:
  * val zkServer = new TestingServer()
  * implicit val system = ActorSystem("test")
  * lazy val zkService = MockZookeeper(zkServer.getConnectString)
  *
  * Now you should be able to call ZK state changing methods in zkService (ZookeeperService.scala).
  * Note: The clusterEnabled flag exists to support wookiee-cluster
  */
object MockZookeeper {
  private[harness] def props(settings:ZookeeperSettings, clusterEnabled: Boolean = false)(implicit system: ActorSystem): Props =
    Props(classOf[TestZookeeperActor], settings, clusterEnabled)

  def apply(zkSettings: ZookeeperSettings, clusterEnabled: Boolean = false)(implicit system: ActorSystem): ZookeeperService = {
    ActorWaitHelper.awaitActor(props(zkSettings, clusterEnabled), system)(Timeout(15 seconds))
    ZookeeperService()
  }

  def apply(config: Config)(implicit system: ActorSystem): ZookeeperService = {
    apply(if (system.settings.config.hasPath("wookiee-zookeeper")) {
      ZookeeperSettings(system.settings.config.getConfig("wookiee-zookeeper"))
    } else {
      ZookeeperSettings(system.settings.config)
    })
  }

  def apply(zookeeperQuorum: String)(implicit system: ActorSystem): ZookeeperService = {
    apply(getTestConfig(zookeeperQuorum))
  }

  def getTestConfig(zookeeperQuorum: String)(implicit system: ActorSystem): ZookeeperSettings = {
    if (!zookeeperQuorum.isEmpty) {
      ZookeeperSettings("test", "pod", zookeeperQuorum)
    } else if (system.settings.config.hasPath("wookiee-zookeeper")) {
      ZookeeperSettings(system.settings.config.getConfig("wookiee-zookeeper"))
    } else {
      ZookeeperSettings(system.settings.config)
    }
  }

  def stop(): Unit = {
    ZookeeperService.getZkActor.foreach(_ ! PoisonPill)
  }
}

case class GetSetWeightInterval()

class TestZookeeperActor(settings: ZookeeperSettings, clusterEnabled: Boolean = false)
  extends ZookeeperActor(settings, clusterEnabled) {
  override def processing: Receive = ( {
    case GetSetWeightInterval => sender() ! setWeightInterval
  }: Receive) orElse super.processing

  log.info(s"Create the TestZookeeperActor and attaching to Zookeeper at ${settings.quorum}")
}
