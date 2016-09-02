package com.webtrends.harness.component.zookeeper

import org.apache.curator.x.discovery.ServiceInstance
import org.apache.curator.x.discovery.details.InstanceSerializer
import org.codehaus.jackson.map.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule


class Serializer extends InstanceSerializer[WookieeService] {
  val sessionMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  override def serialize(instance: ServiceInstance[WookieeService]): Array[Byte] = ???
  override def deserialize(bytes: Array[Byte]): ServiceInstance[WookieeService] = ???
}
