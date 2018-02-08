package com.webtrends.harness.component.zookeeper.discoverable.typed

import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import akka.util.Timeout
import akka.pattern._
import com.webtrends.harness.command._
import com.webtrends.harness.command.typed.{RegisterCommand, TypedCommand, TypedCommandHelper}
import com.webtrends.harness.component.zookeeper.discoverable.Discoverable

import scala.concurrent.Future
import scala.concurrent.duration._

trait DiscoverableTypedCommandHelper extends TypedCommandHelper with Discoverable {
  this: Actor =>
  implicit val basePath:String

  def registerDiscoverableTypedCommandWithProps[T<:TypedCommand[_,_]](name:String, props:Props, id:Option[String]=None,
                                                       checkHealth: Boolean = false) : Future[ActorRef] = {
    implicit val timeout: Timeout = Timeout(2 seconds)

    val idValue = id match {
      case Some(i) => i
      case None => UUID.randomUUID().toString
    }

    getManager().flatMap { tcm =>
      (tcm ? RegisterCommand(name, props, checkHealth)).mapTo[ActorRef].map { r =>
        makeDiscoverable(basePath + "/typed", idValue, name)
        r
      }
    }
  }

  def registerDiscoverableTypedCommand[T<:TypedCommand[_,_]](name:String, actorClass:Class[T], id:Option[String]=None,
                                         checkHealth: Boolean = false) : Future[ActorRef] = {
    registerDiscoverableTypedCommandWithProps(name, Props(actorClass), id, checkHealth)
  }
}

