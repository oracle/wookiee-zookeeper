package com.webtrends.harness.component.zookeeper

import org.specs2.mutable.SpecificationWithJUnit
import NodeRegistration._
import com.webtrends.harness.service.test.config.TestConfig

class NodeRegistrationSpec extends SpecificationWithJUnit {
  "NodeRegistration" should {
    "get base path from config" in {
      val bp = getBasePath(testConf)
      bp mustEqual "/base-path/tdc_tp/1.1"
    }

    "get base path from cluster config" in {
      val bp = getBasePath(testConf.withFallback(
        TestConfig.conf("wookiee-cluster.base-path = /cluster-path")))
      bp mustEqual "/cluster-path/tdc_tp/1.1"
    }
  }

  def testConf = {
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
