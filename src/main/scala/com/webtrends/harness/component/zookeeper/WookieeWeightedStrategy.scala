package com.webtrends.harness.component.zookeeper

import java.util.concurrent.atomic.AtomicInteger

import org.apache.curator.x.discovery.{ServiceInstance, ProviderStrategy}
import org.apache.curator.x.discovery.details.InstanceProvider
import collection.JavaConverters._

class WookieeWeightedStrategy extends ProviderStrategy[WookieeServiceDetails] {
  private val index = new AtomicInteger(0)

  def getInstance(instanceProvider: InstanceProvider[WookieeServiceDetails]): ServiceInstance[WookieeServiceDetails] = {
    val instances = instanceProvider.getInstances.asScala

    if(instances.isEmpty) {
      null
    } else if (instances.map(x => x.getPayload.getWeight).toSet.size == 1) {
      roundRobin(instances)
    } else {
      val headWeight = instances.sortBy(x => x.getPayload.getWeight).head.getPayload.getWeight
      roundRobin(instances.filter(x => x.getPayload.getWeight == headWeight))
    }
  }

  def roundRobin(instances: scala.collection.mutable.Buffer[ServiceInstance[WookieeServiceDetails]]): ServiceInstance[WookieeServiceDetails] = {
    val thisIndex = Math.abs(this.index.getAndIncrement())
    val size = instances.size
    instances(thisIndex % size)
  }
}
