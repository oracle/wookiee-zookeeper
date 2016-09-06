package com.webtrends.harness.component.zookeeper.discoverable

import com.webtrends.harness.command.Command

import scala.concurrent.Future

trait WeightedCommand { this: Discoverable with Command=>
  val basePath: String
  val commandId: String
  def updateWeight(weight: Int): Future[Unit] = updateWeight(weight, basePath, commandName, commandId)
}
