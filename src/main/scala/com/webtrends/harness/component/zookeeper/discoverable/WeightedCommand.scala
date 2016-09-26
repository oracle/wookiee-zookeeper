package com.webtrends.harness.component.zookeeper.discoverable

import com.webtrends.harness.command.Command

import scala.concurrent.Future

case class Weight(weight: Int, forceSet: Boolean)

trait WeightedCommand extends Command {
  this: Discoverable =>

  val basePath: String
  val commandId: String

  def updateWeight(weight: Int, forceSet: Boolean): Future[Boolean] =
    updateWeight(weight, basePath, commandName, commandId, forceSet)

  override def receive =({
    case Weight(w, f) => updateWeight(w, f)
  }: Receive) orElse super.receive
}
