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

import akka.actor.{ActorRef, PoisonPill}
import com.typesafe.config.ConfigFactory
import com.webtrends.harness.app.HActor
import com.webtrends.harness.component.zookeeper.config.ZookeeperSettings
import com.webtrends.harness.component.zookeeper.mock.MockZookeeper
import com.webtrends.harness.utils.ActorWaitHelper
import org.apache.curator.test.TestingServer

import scala.util.Try

trait Zookeeper {
  this: HActor =>
  import Zookeeper._
  implicit val system = context.system
  protected val isMock = Try(system.settings.config
    .getBoolean(ZookeeperManager.ComponentName + ".mock-enabled")).getOrElse(false)

  protected var zkActor: Option[ActorRef] = None

  def startZookeeper(clusterEnabled:Boolean=false) = {
    // Load the zookeeper actor
    if (isMock) {
      log.info("Zookeeper Mock Mode Enabled, Starting Local Test Server...")
      mockZkServer = Some(new TestingServer())
      zkActor = Some(ActorWaitHelper.awaitActor(MockZookeeper.props(zookeeperSettings, clusterEnabled),
        context.system, Some(Zookeeper.ZookeeperName)))
    } else {
      zkActor = Some(ActorWaitHelper.awaitActor(ZookeeperActor.props(zookeeperSettings, clusterEnabled),
        context.system, Some(Zookeeper.ZookeeperName)))
    }
  }

  def stopZookeeper() = {
    zkActor foreach (_ ! PoisonPill)
    mockZkServer foreach (_.close())
  }

  protected def zookeeperSettings: ZookeeperSettings = {
    if (isMock) {
      if (mockZkServer.isEmpty)
        throw new IllegalStateException("Call startZookeeper() to create mockZkServer")
      val conf = ConfigFactory.parseString(ZookeeperManager.ComponentName +
        s""".quorum="${mockZkServer.get.getConnectString}"""")
        .withFallback(config)
      ZookeeperSettings(conf)
    } else {
      ZookeeperSettings(config)
    }
  }
}

object Zookeeper {
  val ZookeeperName = "zookeeper"
  var mockZkServer: Option[TestingServer] = None
}
