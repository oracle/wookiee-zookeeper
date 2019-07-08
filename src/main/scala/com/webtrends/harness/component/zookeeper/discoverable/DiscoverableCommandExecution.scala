package com.webtrends.harness.component.zookeeper.discoverable

import akka.actor.Actor
import akka.pattern.ask
import akka.util.Timeout
import com.webtrends.harness.command.{BaseCommandResponse, CommandException, _}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

/**
 * @author Michael Cuthbert, Spencer Wood
 */
trait DiscoverableCommandExecution extends CommandHelper with Discoverable {
  this: Actor =>
  import context.dispatcher

  /**
   * Executes a discoverable command where ever it may be located
   */
  def executeDiscoverableCommand[T:Manifest](basePath:String, name:String, bean:Option[CommandBean]=None)
                                   (implicit timeout:Timeout) : Future[BaseCommandResponse[T]]= {
    val p = Promise[BaseCommandResponse[T]]
    initCommandManager onComplete {
      case Success(_) =>
        commandManager match {
          case Some(cm) =>
            getInstance(basePath, name) onComplete {
              case Success(in) =>
                (cm ? ExecuteRemoteCommand[T](name, in.getAddress, in.getPort, bean, timeout))(timeout).mapTo[BaseCommandResponse[T]] onComplete {
                  case Success(s) => p success s
                  case Failure(f) => p failure CommandException("CommandManager", f)
                }
              case Failure(f) => p failure CommandException("CommandManager", f)
            }
          case None => p failure CommandException("CommandManager", "CommandManager not found!")
        }
      case Failure(f) => p failure f
    }
    p.future
  }

  /**
    * Executes a discoverable command on every server that is hosting it
    */
  def broadcastDiscoverableCommand[T:Manifest](basePath:String, name:String, bean:Option[CommandBean]=None)
                                            (implicit timeout:Timeout) : Future[BaseCommandResponse[T]]= {
    val p = Promise[BaseCommandResponse[T]]
    initCommandManager onComplete {
      case Success(_) =>
        commandManager match {
          case Some(cm) =>
            getInstances(basePath, name) onComplete {
              case Success(in) if in.nonEmpty =>
                val futures = in.map(i => (cm ? ExecuteRemoteCommand[T](name,
                  i.getAddress, i.getPort, bean, timeout))(timeout).mapTo[BaseCommandResponse[T]])
                Future.sequence(futures) onComplete {
                  case Success(s) => p success CommandResponse[T](Some(s.flatMap(_.data).asInstanceOf[T]))    // TODO - Don't lose response type
                  case Failure(f) => p failure CommandException("CommandManager", f)
                }
              case Failure(f) => p failure CommandException("CommandManager", f)
              case _ => p failure CommandException("CommandManager", new IllegalStateException(s"No instances found for $basePath"))
            }
          case None => p failure CommandException("CommandManager", "CommandManager not found!")
        }
      case Failure(f) => p failure f
    }
    p.future
  }
}
