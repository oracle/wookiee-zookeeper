package com.webtrends.harness.component.zookeeper

import akka.testkit.TestKit
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.webtrends.harness.service.test.TestHarness
import org.specs2.mutable.SpecificationWithJUnit

import scala.concurrent.Await
import scala.concurrent.duration._

class ZookeeperServiceMockSpec
  extends SpecificationWithJUnit {

  val testHarness = TestHarness(ConfigFactory.parseString(
    """
      |wookiee-zookeeper {
      |  enabled = true
      |  mock-enabled = true
      |  base-path="/test_path"
      |}
    """.stripMargin), None, None)
  implicit val system = TestHarness.system.get
  val service = ZookeeperService()

  implicit val to = Timeout(2 seconds)
  val awaitResultTimeout = 5000 milliseconds

  sequential

  "The zookeeper service" should {
    "allow callers to create a node for a valid path" in {
      val res = Await.result(service.createNode("/test", ephemeral = false, Some("data".getBytes)), awaitResultTimeout)
      res shouldEqual "/test"
    }

    "allow callers to create a node for a valid namespace and path" in {
      val res = Await.result(service.createNode("/namespacetest", ephemeral = false, Some("namespacedata".getBytes), Some("space")), awaitResultTimeout)
      res shouldEqual "/namespacetest"
    }

    "allow callers to delete a node for a valid path" in {
      val res = Await.result(service.createNode("/deleteTest", ephemeral = false, Some("data".getBytes)), awaitResultTimeout)
      res shouldEqual "/deleteTest"
      val res2 = Await.result(service.deleteNode("/deleteTest"), awaitResultTimeout)
      res2 shouldEqual "/deleteTest"
    }

    "allow callers to delete a node for a valid namespace and path " in {
      val res = Await.result(service.createNode("/deleteTest", ephemeral = false, Some("data".getBytes), Some("space")), awaitResultTimeout)
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
      Await.result(service.createNode("/test/child", ephemeral = false, None), awaitResultTimeout)
      val res2 = Await.result(service.getChildren("/test"), awaitResultTimeout)
      res2.head._1 shouldEqual "child"
      res2.head._2 shouldEqual None
    }

    " allow callers to get children with data for a valid path " in {
      Await.result(service.setData("/test/child", "data".getBytes), awaitResultTimeout)
      val res2 = Await.result(service.getChildren("/test", includeData = true), awaitResultTimeout)
      res2.head._1 shouldEqual "child"
      res2.head._2.get shouldEqual "data".getBytes
    }

    " return an error when getting children for an invalid path " in {
      Await.result(service.getChildren("/testbad"), awaitResultTimeout) must throwA[Exception]
    }
  }

  step {
    TestKit.shutdownActorSystem(system)
  }
}
