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

import akka.actor.{ActorSystem, Identify}
import akka.pattern.ask
import akka.testkit.TestKit
import akka.util.Timeout
import com.webtrends.harness.component.zookeeper.config.ZookeeperSettings
import com.webtrends.harness.component.zookeeper.discoverable.DiscoverableService
import org.apache.curator.test.TestingServer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.curator.x.discovery.UriSpec
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.time.NoTimeConversions

import scala.concurrent.Await
import scala.concurrent.duration._

class DiscoverableServiceSpec
  extends SpecificationWithJUnit with NoTimeConversions {

  val path = "/discovery/test"
  val testServiceName = "TestService"

  val zkServer = new TestingServer()
  implicit val system = ActorSystem("test", loadConfig)

  lazy val zkActor = system.actorOf(ZookeeperActor.props(ZookeeperSettings(system.settings.config.getConfig("wookiee-zookeeper"))))
  implicit val to = Timeout(2 seconds)

  Await.result(zkActor ? Identify("xyz123"), 2 seconds)
  lazy val service = DiscoverableService()
  Thread.sleep(5000)
  sequential

  "The discoverable service" should {

    " make a service discoverable " in {
      val res = Await.result(service.makeDiscoverable(path, "d3f2248f-2652-4ce5-9caa-5ea2ed28b1a5", testServiceName, None,
        2552,
        new UriSpec(s"akka.tcp://server@[Server]:2552/$testServiceName")), 1000 milliseconds)
      res must be equalTo true
    }

    " get an instance of a discoverable service" in {
      val res2 = Await.result(service.getInstance(path, testServiceName), 1000 milliseconds)
      res2.getName must be equalTo testServiceName
    }
  }

  step {
    TestKit.shutdownActorSystem(system)
    zkServer.close
  }

  def loadConfig: Config = {
    ConfigFactory.parseString("""
      wookiee-zookeeper {
        quorum = "%s"
      }
                              """.format(zkServer.getConnectString)
    ).withFallback(ConfigFactory.load()).resolve
  }
}
