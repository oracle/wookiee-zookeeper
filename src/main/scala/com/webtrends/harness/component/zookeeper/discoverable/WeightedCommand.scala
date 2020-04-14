package com.webtrends.harness.component.zookeeper.discoverable

import com.webtrends.harness.command.Command

import scala.concurrent.Future
import scala.reflect.ClassTag

case class Weight(weight: Int, forceSet: Boolean)

abstract class WeightedCommand[Input <: Product : ClassTag, Output <: Any : ClassTag]
  extends Command[Input, Output] {
  this: Discoverable =>

  val basePath: String
  val commandId: String

  def updateWeight(weight: Int, forceSet: Boolean): Future[Boolean] =
    updateWeight(weight, basePath, commandId, forceSet)

  override def receive: Receive =({
    case Weight(w, f) => updateWeight(w, f)
  }: Receive) orElse super.receive
}
