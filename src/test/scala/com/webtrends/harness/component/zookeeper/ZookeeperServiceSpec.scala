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

import java.util.UUID

import akka.actor.Actor.Receive
import akka.actor._
import akka.pattern.ask
import akka.testkit.TestKit
import akka.util.Timeout
import com.webtrends.harness.component.zookeeper.config.ZookeeperSettings
import com.webtrends.harness.component.zookeeper.discoverable.DiscoverableService.{UpdateWeight, QueryForInstances, MakeDiscoverable}
import org.apache.curator.test.TestingServer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.curator.x.discovery.{ServiceInstance, UriSpec}
import org.joda.time.DateTime
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.time.NoTimeConversions

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.util.{Success, Failure}

class ZookeeperServiceSpec
  extends SpecificationWithJUnit with NoTimeConversions {

  val zkServer = new TestingServer()
  implicit val system = ActorSystem("test", loadConfig)

  lazy val zkActor = system.actorOf(TestZookeeperActor.props(ZookeeperSettings(system.settings.config.getConfig("wookiee-zookeeper"))))

  implicit val to = Timeout(2 seconds)
  val awaitResultTimeout = 5000 milliseconds

  Await.result(zkActor ? Identify("xyz123"), 2 seconds)
  lazy val service = ZookeeperService()
  Thread.sleep(5000)
  sequential

  "The zookeeper service" should {

    "allow callers to create a node for a valid path" in {
      val res = Await.result(service.createNode("/test", false, Some("data".getBytes)), awaitResultTimeout)
      res shouldEqual "/test"
    }

    "allow callers to create a node for a valid namespace and path" in {
      val res = Await.result(service.createNode("/namespacetest", false, Some("namespacedata".getBytes), Some("space")), awaitResultTimeout)
      res shouldEqual "/namespacetest"
    }

    "allow callers to delete a node for a valid path" in {
      val res = Await.result(service.createNode("/deleteTest", false, Some("data".getBytes)), awaitResultTimeout)
      res shouldEqual "/deleteTest"
      val res2 = Await.result(service.deleteNode("/deleteTest"), awaitResultTimeout)
      res2 shouldEqual "/deleteTest"
    }

    "allow callers to delete a node for a valid namespace and path " in {
      val res = Await.result(service.createNode("/deleteTest", false, Some("data".getBytes), Some("space")), awaitResultTimeout)
      res shouldEqual "/deleteTest"
      val res2 = Await.result(service.deleteNode("/deleteTest", Some("space")), awaitResultTimeout)
      res2 shouldEqual "/deleteTest"
    }

    "allow callers to get data for a valid path " in {
      val res = Await.result(service.getData("/test"), awaitResultTimeout)
      new String(res) shouldEqual "data"
    }

    "allow callers to get data for a valid namespace and path " in {
      val res = Await.result(service.getData("/namespacetest", Some("space")), awaitResultTimeout)
      new String(res) shouldEqual "namespacedata"
    }

    " allow callers to get data for a valid path with a namespace" in {
      val res = Await.result(service.getData("/namespacetest", Some("space")), awaitResultTimeout)
      new String(res) shouldEqual "namespacedata"
    }

    " return an error when getting data for an invalid path " in {
      Await.result(service.getData("/testbad"), awaitResultTimeout) must throwA[Exception]
    }

    " allow callers to get children with no data for a valid path " in {
      val res = Await.result(service.createNode("/test/child", false, None), awaitResultTimeout)
      val res2 = Await.result(service.getChildren("/test", false), awaitResultTimeout)
      res2(0)._1 shouldEqual "child"
      res2(0)._2 shouldEqual None
    }

    " allow callers to get children with data for a valid path " in {
      val res = Await.result(service.setData("/test/child", "data".getBytes), awaitResultTimeout)
      val res2 = Await.result(service.getChildren("/test", true), awaitResultTimeout)
      res2(0)._1 shouldEqual "child"
      res2(0)._2.get shouldEqual "data".getBytes
    }

    " return an error when getting children for an invalid path " in {
      Await.result(service.getChildren("/testbad"), awaitResultTimeout) must throwA[Exception]
    }

    "allow callers to discover commands " in {
      val res = Await.result(zkActor ? MakeDiscoverable("base/path", "id", "testname", None, 8080, new UriSpec("file://foo")), awaitResultTimeout)
      res.asInstanceOf[Boolean] mustEqual true
    }

    "have default weight set to 0" in {
      val basePath = "base/path"
      val id = UUID.randomUUID().toString
      val name = UUID.randomUUID().toString

      Await.result(zkActor ? MakeDiscoverable(basePath, id, name, None, 8080, new UriSpec("file://foo")), awaitResultTimeout)

      val res2 = Await.result(zkActor ? QueryForInstances(basePath, name, Some(id)), awaitResultTimeout)
      res2.asInstanceOf[ServiceInstance[WookieeServiceDetails]].getPayload.getWeight mustEqual 0
    }

    "update weight " in {
      val basePath = "base/path"
      val id = UUID.randomUUID().toString
      val name = UUID.randomUUID().toString

      Await.result(zkActor ? MakeDiscoverable(basePath, id, name, None, 8080, new UriSpec("file://foo")), awaitResultTimeout)
      Await.result(zkActor ? UpdateWeight(100, basePath, name, id, false), awaitResultTimeout)

      def result = {
        val r = Await.result(zkActor ? QueryForInstances(basePath, name, Some(id)), awaitResultTimeout)
        r.asInstanceOf[ServiceInstance[WookieeServiceDetails]]
      }

      result.getPayload.getWeight must be_==(100).eventually(2, 6 seconds)
    }

    "only update weight on a set interval " in {
      val basePath = "base/path"
      val id = UUID.randomUUID().toString
      val name = UUID.randomUUID().toString

      Await.result(zkActor ? MakeDiscoverable(basePath, id, name, None, 8080, new UriSpec("file://foo")), awaitResultTimeout)

      zkActor ! UpdateWeight(100, basePath, name, id, false)
      Thread.sleep(3000)
      zkActor ! UpdateWeight(100, basePath, name, id, false)
      Thread.sleep(3000)


      val times = Await.result(zkActor ? GetSetWeightTimes, awaitResultTimeout).asInstanceOf[Seq[DateTime]]
      val timeDiffs = times
        .sliding(2)
        .map{case Seq(x, y, _*) => Math.round((y.getMillis - x.getMillis) / 1000.0)}
        .toSet

      timeDiffs.size mustEqual 1
      timeDiffs.head mustEqual 2
    }

    "weight is updated in zookeeper if forceSet is true " in {
      val basePath = "base/path"
      val id = UUID.randomUUID().toString
      val name = UUID.randomUUID().toString

      Await.result(zkActor ? MakeDiscoverable(basePath, id, name, None, 8080, new UriSpec("file://foo")), awaitResultTimeout)
      Await.result(zkActor ? UpdateWeight(100, basePath, name, id, true), awaitResultTimeout)

      val res = Await.result(zkActor ? QueryForInstances(basePath, name, Some(id)), awaitResultTimeout).asInstanceOf[ServiceInstance[WookieeServiceDetails]]

      res.getPayload.getWeight mustEqual 100

    }

    "use set weight interval defined in config" in {
      Await.result(zkActor ? GetSetWeightInterval, 3 second).asInstanceOf[Long] mustEqual 2
    }
  }

  step {
    TestKit.shutdownActorSystem(system)
    zkServer.close
  }

  def loadConfig: Config = {
    ConfigFactory.parseString("""
      discoverability {
        set-weight-interval = 2s
      }
      wookiee-zookeeper {
        quorum = "%s"
      }
                              """.format(zkServer.getConnectString)
    ).withFallback(ConfigFactory.load()).resolve
  }
}

class WeightWatcherActor(zkActor: ActorRef, basePath: String, name: String, id: String) extends Actor {
  import context.dispatcher
  implicit val timeout = Timeout.durationToTimeout(10 seconds)

  var weight = Seq.empty[Int]
  context.system.scheduler.schedule(0 milliseconds, 100 milliseconds, self, "msg")

  override def receive: Actor.Receive = {
    case "msg" =>
      zkActor ? QueryForInstances(basePath, name, Some(id)) onComplete {
        case Success(s) =>
          val w = s.asInstanceOf[ServiceInstance[WookieeServiceDetails]].getPayload.getWeight
          weight = weight ++ Seq(w)
        case Failure(f) =>
      }
    case "getSample" => sender() ! weight

    case _ =>
  }
}

class WeightUpdateActor(zkActor: ActorRef, basePath: String, name: String, id: String) extends Actor {
  import context.dispatcher

  context.system.scheduler.schedule(0 milliseconds, 1 second, self, "msg")
  var weight = 100
  override def receive: Receive = {
    case "msg" =>
      zkActor ! UpdateWeight(weight, basePath, name, id, false)
      weight = weight - 1
    case _ =>
  }
}