package com.webtrends.harness.component.zookeeper.discoverable

import java.util.UUID

import akka.actor.{ActorRef, Props, Actor}
import akka.util.Timeout
import scala.concurrent.duration._
import akka.pattern.ask
import com.webtrends.harness.command._

import scala.concurrent.{Promise, Future}
import scala.util.{Failure, Success}

/**
 * @author Michael Cuthbert on 7/10/15.
 */
trait DiscoverableCommandHelper extends CommandHelper with Discoverable {
  this: Actor =>
  import context.dispatcher
  implicit val basePath:String

  /**
   * Wrapper that allows services to add commands to the command manager with a single discoverable command
   *
   * @param name name of the command you want to add
   * @param props the props for that command actor class
   * @param id id
   * @return
   */
  def addDiscoverableCommandWithProps[T<:Command](name:String, props:Props, id:Option[String]=None) : Future[Boolean] = {
    addDiscoverable(name, props, id)
  }

  /**
   * Wrapper that allows services add commands to the command manager with a single discoverable command
   *
   * @param name name of the command you want to add
   * @param actorClass the class for the actor
   * @param id id
   */
  def addDiscoverableCommand[T<:Command](name:String, actorClass:Class[T], id:Option[String]=None) : Future[Boolean] = {
    addDiscoverable(name, Props(actorClass), id)
  }

  private def addDiscoverable[T<:Command](name: String, props: Props, id: Option[String] = None): Future[Boolean] = {
    implicit val timeout = Timeout(5 seconds)
    val idValue = id.getOrElse(UUID.randomUUID().toString)
    val m = AddCommandWithProps(name, props)

    val result = for {
      _ <- initCommandManager
      _ <- commandManager.map(_ ? m).getOrElse(Future.failed(new CommandException("CommandManager", "CommandManager not found!")))
      r <- makeDiscoverable(basePath, idValue, name)
    } yield {
      r
    }

    result.onFailure { case t => log.error("Failed to add discoverable command", t) }
    result
  }
}
