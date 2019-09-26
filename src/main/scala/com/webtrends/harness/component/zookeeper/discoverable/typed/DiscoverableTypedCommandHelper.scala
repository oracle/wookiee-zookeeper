package com.webtrends.harness.component.zookeeper.discoverable.typed

import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import akka.util.Timeout
import akka.pattern._
import com.webtrends.harness.command.typed.{RegisterCommand, TypedCommand, TypedCommandHelper}
import com.webtrends.harness.component.zookeeper.discoverable.Discoverable
import com.webtrends.harness.logging.ActorLoggingAdapter

import scala.concurrent.Future
import scala.concurrent.duration._

trait DiscoverableTypedCommandHelper extends TypedCommandHelper with Discoverable with ActorLoggingAdapter {
  this: Actor =>
  implicit val basePath:String

  def registerDiscoverableTypedCommandWithProps[T<:TypedCommand[_,_]](name:String, props:Props, id:Option[String]=None,
                                                       checkHealth: Boolean = false) : Future[Boolean] = {
    implicit val timeout: Timeout = Timeout(5 seconds)
    val idValue = id.getOrElse(UUID.randomUUID().toString)

    val result = for {
      tcm <- getManager()
      _ <- tcm ? RegisterCommand(name, props, checkHealth)
      r <- makeDiscoverable(basePath + "/typed", idValue, name)
    } yield {
      r
    }

    result.onFailure { case t => log.error("Failed to add discoverable command", t) }
    result

  }

  def registerDiscoverableTypedCommand[T<:TypedCommand[_,_]](name:String, actorClass:Class[T], id:Option[String]=None,
                                         checkHealth: Boolean = false) : Future[Boolean] = {
    registerDiscoverableTypedCommandWithProps(name, Props(actorClass), id, checkHealth)
  }
}

