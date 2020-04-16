package com.webtrends.harness.component.zookeeper.discoverable

import akka.actor.Actor
import akka.util.Timeout
import com.webtrends.harness.component.zookeeper.WookieeServiceDetails
import org.apache.curator.x.discovery.{ServiceInstance, UriSpec}

import scala.concurrent.Future

/**
 * @author Michael Cuthbert and Spencer Wood
 */
trait Discoverable {
  this: Actor =>

  import context.system
  private lazy val service = DiscoverableService()
  val port: Int = context.system.settings.config.getInt("akka.remote.netty.tcp.port")
  val address: String = context.system.settings.config.getString("akka.remote.netty.tcp.hostname")

  def queryForNames(basePath:String)(implicit timeout:Timeout): Future[List[String]] =
    service.queryForNames(basePath)

  def queryForInstances(basePath: String, id: String)
                       (implicit timeout:Timeout): Future[List[ServiceInstance[WookieeServiceDetails]]] =
    service.queryForInstances(basePath, id)

  def makeDiscoverable(basePath: String, id: String)(implicit timeout:Timeout): Future[Boolean] =
    makeDiscoverable(basePath, id, Some(address), port,
      new UriSpec(s"akka.tcp://server@$address:$port/${context.system.name}"))

  def makeDiscoverable(
                        basePath: String,
                        id: String,
                        address: Option[String],
                        port: Int,
                        uriSpec: UriSpec)(implicit timeout:Timeout): Future[Boolean] =
    service.makeDiscoverable(basePath, id, address, port, uriSpec)

  def getInstances(basePath:String, id:String)
                  (implicit timeout:Timeout): Future[List[ServiceInstance[WookieeServiceDetails]]] =
    service.getAllInstances(basePath, id)

  def getInstance(basePath:String, id:String)
                 (implicit timeout:Timeout): Future[ServiceInstance[WookieeServiceDetails]] =
    service.getInstance(basePath, id)

  def updateWeight(weight: Int, basePath:String, id: String, forceSet: Boolean = false)
                  (implicit timeout:Timeout): Future[Boolean] =
    service.updateWeight(weight, basePath, id, forceSet)
}
