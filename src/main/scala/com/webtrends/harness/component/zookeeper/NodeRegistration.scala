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

import java.net.InetAddress

import akka.actor.ActorSystem
import com.typesafe.config.Config
import com.webtrends.harness.component.zookeeper.config.ZookeeperSettings

object NodeRegistration {
  /**
    * Get the node/cluster base path
    * @param config current ZK config
    * @return the base path
    */
  def getBasePath(config: Config): String = {
    val zookeeperSettings = ZookeeperSettings(config)
    val basePath = if (config.hasPath("wookiee-cluster.base-path")) {
      config.getConfig("wookiee-cluster").getString("base-path")
    } else {
      zookeeperSettings.basePath
    }
    getBasePath(zookeeperSettings.copy(basePath = basePath))
  }

  /**
   * Get the node/cluster base path
   * @param zookeeperSettings current ZK settings
   * @return the base path
   */
  def getBasePath(zookeeperSettings: ZookeeperSettings): String = {
    s"${zookeeperSettings.basePath}/${zookeeperSettings.dataCenter}_${zookeeperSettings.pod}/${zookeeperSettings.version}"
  }

  /**
    * Return the address name this node will carry
    * @return
    */
  def getAddress(implicit system: ActorSystem): String = {
    val address = SystemExtension(system).address
    val port = if (address.port.isDefined) address.port.get
      else system.settings.config.getInt("akka.remote.netty.tcp.port")
    val addrHost = address.host

    val host = if (addrHost.isEmpty) {
      InetAddress.getLocalHost.getCanonicalHostName
    } else if (!Zookeeper.isMock(system.settings.config) &&
      (addrHost.get.equalsIgnoreCase("localhost") || addrHost.get.equals("127.0.0.1"))) {
      InetAddress.getLocalHost.getCanonicalHostName
    } else {
      addrHost.get
    }

    s"$host:$port"
  }
}
