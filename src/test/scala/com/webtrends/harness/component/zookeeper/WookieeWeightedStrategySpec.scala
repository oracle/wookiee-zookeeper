package com.webtrends.harness.component.zookeeper

import java.util
import java.util.UUID

import org.apache.curator.x.discovery.{UriSpec, ServiceInstance}
import org.apache.curator.x.discovery.details.InstanceProvider
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.time.NoTimeConversions
import collection.JavaConverters._

class WookieeWeightedStrategySpec extends SpecificationWithJUnit with NoTimeConversions {

  class MockInstanceProvider(instances: Seq[ServiceInstance[WookieeServiceDetails]]) extends InstanceProvider[WookieeServiceDetails] {
    override def getInstances: util.List[ServiceInstance[WookieeServiceDetails]] = instances.toList.asJava
  }

  "WookieeWeightedStrategy" should {
    val builder = ServiceInstance.builder[WookieeServiceDetails]()
      .uriSpec(new UriSpec(s"akka.tcp://server@localhost:8080/"))

    "returns null when no instances" in {
      val instances = Seq.empty[ServiceInstance[WookieeServiceDetails]]

      val instanceProvider = new MockInstanceProvider(instances)
      val strategy = new WookieeWeightedStrategy()

      strategy.getInstance(instanceProvider) mustEqual null
    }

    "default to round-robin when weights are all the same" in {
      val instances = (0 to 10).map { index =>
        builder
          .id(index.toString)
          .name(UUID.randomUUID().toString)
          .payload(new WookieeServiceDetails(0))
          .port(8080)
          .build()
      }

      val instanceProvider = new MockInstanceProvider(instances)
      val strategy = new WookieeWeightedStrategy()

      (0 to 10).map(i => strategy.getInstance(instanceProvider).getId == i.toString).reduce(_ && _) mustEqual true
    }

    "pick the lowest weighted instance" in {
      val instances = (1 to 10).map { index =>
        builder
          .id(index.toString)
          .name(UUID.randomUUID().toString)
          .payload(new WookieeServiceDetails(index))
          .port(8080)
          .build()
      } ++ Seq(builder
              .id("0")
              .name(UUID.randomUUID().toString)
              .payload(new WookieeServiceDetails(0))
              .port(8080)
              .build())

      val instanceProvider = new MockInstanceProvider(instances)
      val strategy = new WookieeWeightedStrategy()

      strategy.getInstance(instanceProvider).getId mustEqual "0"
    }
  }
}
