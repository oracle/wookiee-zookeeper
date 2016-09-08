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

  lazy val zkActor = system.actorOf(ZookeeperActor.props(ZookeeperSettings(system.settings.config.getConfig("wookiee-zookeeper"))))
  implicit val to = Timeout(2 seconds)

  Await.result(zkActor ? Identify("xyz123"), 2 seconds)
  lazy val service = ZookeeperService()
  Thread.sleep(5000)
  sequential

  "The zookeeper service" should {

    "allow callers to create a node for a valid path" in {
      val res = Await.result(service.createNode("/test", false, Some("data".getBytes)), 1000 milliseconds)
      res shouldEqual "/test"
    }

    "allow callers to create a node for a valid namespace and path" in {
      val res = Await.result(service.createNode("/namespacetest", false, Some("namespacedata".getBytes), Some("space")), 1000 milliseconds)
      res shouldEqual "/namespacetest"
    }

    "allow callers to delete a node for a valid path" in {
      val res = Await.result(service.createNode("/deleteTest", false, Some("data".getBytes)), 1000 milliseconds)
      res shouldEqual "/deleteTest"
      val res2 = Await.result(service.deleteNode("/deleteTest"), 1000 milliseconds)
      res2 shouldEqual "/deleteTest"
    }

    "allow callers to delete a node for a valid namespace and path " in {
      val res = Await.result(service.createNode("/deleteTest", false, Some("data".getBytes), Some("space")), 1000 milliseconds)
      res shouldEqual "/deleteTest"
      val res2 = Await.result(service.deleteNode("/deleteTest", Some("space")), 1000 milliseconds)
      res2 shouldEqual "/deleteTest"
    }

    "allow callers to get data for a valid path " in {
      val res = Await.result(service.getData("/test"), 1000 milliseconds)
      new String(res) shouldEqual "data"
    }

    "allow callers to get data for a valid namespace and path " in {
      val res = Await.result(service.getData("/namespacetest", Some("space")), 1000 milliseconds)
      new String(res) shouldEqual "namespacedata"
    }

    " allow callers to get data for a valid path with a namespace" in {
      val res = Await.result(service.getData("/namespacetest", Some("space")), 1000 milliseconds)
      new String(res) shouldEqual "namespacedata"
    }

    " return an error when getting data for an invalid path " in {
      Await.result(service.getData("/testbad"), 1000 milliseconds) must throwA[Exception]
    }

    " allow callers to get children with no data for a valid path " in {
      val res = Await.result(service.createNode("/test/child", false, None), 1000 milliseconds)
      val res2 = Await.result(service.getChildren("/test", false), 1000 milliseconds)
      res2(0)._1 shouldEqual "child"
      res2(0)._2 shouldEqual None
    }

    " allow callers to get children with data for a valid path " in {
      val res = Await.result(service.setData("/test/child", "data".getBytes), 1000 milliseconds)
      val res2 = Await.result(service.getChildren("/test", true), 1000 milliseconds)
      res2(0)._1 shouldEqual "child"
      res2(0)._2.get shouldEqual "data".getBytes
    }

    " return an error when getting children for an invalid path " in {
      Await.result(service.getChildren("/testbad"), 1000 milliseconds) must throwA[Exception]
    }

    "allow callers to discover commands " in {
      val res = Await.result(zkActor ? MakeDiscoverable("base/path", "id", "testname", None, 8080, new UriSpec("file://foo")), 1 seconds)
      res.asInstanceOf[Boolean] mustEqual true
    }

    "have default weight set to 0" in {
      val basePath = "base/path"
      val id = UUID.randomUUID().toString
      val name = UUID.randomUUID().toString

      Await.result(zkActor ? MakeDiscoverable(basePath, id, name, None, 8080, new UriSpec("file://foo")), 1 seconds)

      val res2 = Await.result(zkActor ? QueryForInstances(basePath, name, Some(id)), 1 seconds)
      res2.asInstanceOf[ServiceInstance[WookieeServiceDetails]].getPayload.getWeight mustEqual 0
    }

    "update weight " in {
      val basePath = "base/path"
      val id = UUID.randomUUID().toString
      val name = UUID.randomUUID().toString

      Await.result(zkActor ? MakeDiscoverable(basePath, id, name, None, 8080, new UriSpec("file://foo")), 1 seconds)
      Await.result(zkActor ? UpdateWeight(100, basePath, name, id), 1 seconds)

      def result = {
        val r = Await.result(zkActor ? QueryForInstances(basePath, name, Some(id)), 1 seconds)
        r.asInstanceOf[ServiceInstance[WookieeServiceDetails]]
      }

      result.getPayload.getWeight must be_==(100).eventually(2, 6 seconds)
    }

//    "only update weight on a set interval " in {
//      val basePath = "base/path"
//      val id = UUID.randomUUID().toString
//      val name = UUID.randomUUID().toString
//
//      Await.result(zkActor ? MakeDiscoverable(basePath, id, name, None, 8080, new UriSpec("file://foo")), 1 seconds)
//
//      // Send update weight messages
//      val watcherActor = system.actorOf(Props(classOf[WeightWatcherActor], zkActor, basePath, name, id))
//      val updateActor = system.actorOf(Props(classOf[WeightUpdateActor], zkActor, basePath, name, id))
//
//
//      Await.reswatcherActor ? "getSample"
//      val f = Future {
//        Thread.sleep(60000)
//        1
//      }
//
//      Await.result(f, 30 seconds) mustEqual 1
//    }
//
//    "use set weight interval defined in config" in {
//      failure("todo")
//    }
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
  context.system.scheduler.schedule(0 milliseconds, 2000 milliseconds, self, "msg")

  override def receive: Actor.Receive = {
    case "msg" =>
      zkActor ? QueryForInstances(basePath, name, Some(id)) onComplete {
        case Success(s) =>
          val p = s.asInstanceOf[ServiceInstance[WookieeServiceDetails]].getPayload
          weight = weight ++ Seq(p.getWeight)
        case Failure(f) =>
      }
    case "getSample" => sender() ! weight

    case _ =>
  }
}
class WeightUpdateActor(zkActor: ActorRef, basePath: String, name: String, id: String) extends Actor {
  import context.dispatcher

  context.system.scheduler.schedule(0 milliseconds, 1000 milliseconds, self, "msg")
  var weight = 100
  override def receive: Receive = {
    case "msg" =>
      zkActor ! UpdateWeight(weight, basePath, name, id)
      weight = weight - 1
    case _ =>
  }
}