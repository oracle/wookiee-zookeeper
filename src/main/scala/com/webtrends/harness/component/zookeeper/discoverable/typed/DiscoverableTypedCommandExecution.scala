package com.webtrends.harness.component.zookeeper.discoverable.typed

import akka.actor.{ActorContext, ActorRef, ActorSystem}
import akka.util.Timeout
import akka.pattern._
import com.webtrends.harness.HarnessConstants
import com.webtrends.harness.command.typed.ExecuteTypedCommand
import com.webtrends.harness.component.zookeeper.discoverable.DiscoverableService

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import com.webtrends.harness.utils.FutureExtensions._

object DiscoverableTypedCommandExecution {

  implicit var service: DiscoverableService = _
  implicit var system: ActorSystem = _
  implicit var ec: ExecutionContext = _

  def init(context: ActorContext): Unit = {
    system = context.system
    service = DiscoverableService()(system)
    ec = context.dispatcher
  }

  def execute[U, V](discPath: String, name: String, args: U)(implicit ctx: ActorContext, to: Timeout): Future[V] = {
    getRemoteActorRef(s"$discPath/typed", name, ctx)(to).flatMapAll[V] {
      case Success(commandActor) =>
        (commandActor ? ExecuteTypedCommand(args)).map(_.asInstanceOf[V])
      case Failure(f) =>
        Future.failed(new IllegalArgumentException(s"Unable to find remote actor for command $name at $discPath.", f))
    }
  }

  private def getRemoteActorRef(discoveryPath: String, name: String, context: ActorContext)(implicit to: Timeout): Future[ActorRef] = {
    service.getInstance(discoveryPath, name).flatMap { r =>
      context.actorSelection(remotePath(r.getAddress, r.getPort, name)).resolveOne()
    }
  }

  private def remotePath(server: String, port: Int, name: String): String = {
    s"akka.tcp://server@$server:$port${HarnessConstants.TypedCommandFullName}/$name"
  }
}
