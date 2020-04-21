package com.webtrends.harness.component.zookeeper

import com.typesafe.config.Config
import com.webtrends.harness.component.zookeeper.NodeRegistration._
import com.webtrends.harness.service.test.config.TestConfig
import org.scalatest.{Matchers, WordSpecLike}

class NodeRegistrationSpec extends WordSpecLike with Matchers {
  "NodeRegistration" should {
    "get base path from config" in {
      val bp = getBasePath(testConf)
      bp shouldBe "/base-path/tdc_tp/1.1"
    }

    "get base path from cluster config" in {
      val bp = getBasePath(testConf.withFallback(
        TestConfig.conf("wookiee-cluster.base-path = /cluster-path")))
      bp shouldBe "/cluster-path/tdc_tp/1.1"
    }
  }

  def testConf: Config = {
    TestConfig.conf(
      """
        |wookiee-zookeeper {
        | quorum = "fake"
        | base-path = "/base-path"
        | datacenter = "tdc"
        | pod = "tp"
        |}
      """.stripMargin)
  }
}
