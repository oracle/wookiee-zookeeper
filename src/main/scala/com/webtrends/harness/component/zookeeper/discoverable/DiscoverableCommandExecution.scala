package com.webtrends.harness.component.zookeeper.discoverable

import akka.actor.Actor
import akka.pattern.ask
import akka.util.Timeout
import com.webtrends.harness.command._

import scala.concurrent.{Promise, Future}
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
                                   (implicit timeout:Timeout) : Future[CommandResponse[T]]= {
    val p = Promise[CommandResponse[T]]
    initCommandManager onComplete {
      case Success(_) =>
        commandManager match {
          case Some(cm) =>
            getInstance(basePath, name) onComplete {
              case Success(in) =>
                (cm ? ExecuteRemoteCommand[T](name, in.getAddress, in.getPort, bean, timeout))(timeout).mapTo[CommandResponse[T]] onComplete {
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
}
