package com.webtrends.harness.component.zookeeper

import akka.actor.{ActorRef, ActorSystem, Identify}
import akka.pattern.ask
import akka.testkit.TestKit
import akka.util.Timeout
import com.webtrends.harness.component.zookeeper.config.ZookeeperSettings
import com.webtrends.harness.component.zookeeper.discoverable.DiscoverableService
import org.apache.curator.test.TestingServer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.curator.x.discovery.UriSpec
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class DiscoverableServiceSpec
  extends WordSpecLike with Matchers with BeforeAndAfterAll {

  val path = "/discovery/test"
  val testServiceName = "TestService"

  val zkServer = new TestingServer()
  implicit val system: ActorSystem = ActorSystem("test", loadConfig)

  "The discoverable service" should {
    lazy val zkActor: ActorRef = system.actorOf(ZookeeperActor.props(ZookeeperSettings(system.settings.config)))
    implicit val to: Timeout = Timeout(5 seconds)

    Await.result(zkActor ? Identify("xyz123"), 5 seconds)
    lazy val service: DiscoverableService = DiscoverableService()
    Thread.sleep(5000)

    " make a service discoverable " in {
      val res = Await.result(service.makeDiscoverable(path, testServiceName, None, 2552,
        new UriSpec(s"akka.tcp://server@[Server]:2552/$testServiceName")), 2000 milliseconds)
      res shouldBe true
    }

    " get an instance of a discoverable service" in {
      val res2 = Await.result(service.getInstance(path, testServiceName), 2000 milliseconds)
      res2.getName shouldBe testServiceName
    }
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    zkServer.close()
  }

  def loadConfig: Config = {
    ConfigFactory.parseString("""
      wookiee-zookeeper {
        quorum = "%s"
      }""".format(zkServer.getConnectString)
    ).withFallback(ConfigFactory.load()).resolve
  }
}
