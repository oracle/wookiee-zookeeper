package com.webtrends.harness.component.zookeeper.discoverable

import com.webtrends.harness.command.Command

import scala.concurrent.Future

case class Weight(weight: Int)

trait WeightedCommand extends Command {
  this: Discoverable =>

  val basePath: String
  val commandId: String

  def updateWeight(weight: Int): Future[Boolean] = updateWeight(weight, basePath, commandName, commandId)

  override def receive =({
    case Weight(w) => updateWeight(w)
  }: Receive) orElse super.receive
}
