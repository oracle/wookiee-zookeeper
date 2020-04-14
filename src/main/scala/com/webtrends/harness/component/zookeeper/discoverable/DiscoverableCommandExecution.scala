package com.webtrends.harness.component.zookeeper.discoverable

import akka.actor.Actor
import akka.pattern.ask
import akka.util.Timeout
import com.webtrends.harness.command.{CommandException, _}

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * @author Michael Cuthbert, Spencer Wood
 */
trait DiscoverableCommandExecution extends CommandHelper with Discoverable {
  this: Actor =>
  import context.dispatcher

  /**
   * Executes a discoverable command where ever it may be located
   */
  def executeDiscoverableCommand[Input <: Product : ClassTag, Output <: Any : ClassTag]
    (basePath: String, id: String, bean: Input)(implicit timeout: Timeout): Future[Output]= {

    initCommandManager flatMap { cm =>
      getInstance(basePath, id) flatMap { in =>
        (cm ? ExecuteRemoteCommand[Input](id, in.getAddress, in.getPort, bean, timeout)) (timeout).mapTo[Output]
      }
    } recover {
      case f: Throwable =>
        throw CommandException("CommandManager", f)
    }
  }

  /**
    * Executes a discoverable command on every server that is hosting it
    */
  def broadcastDiscoverableCommand[Input <: Product : ClassTag, Output <: Any : ClassTag]
    (basePath: String, id: String, bean: Input)(implicit timeout: Timeout): Future[Iterable[Output]]= {

    initCommandManager flatMap { cm =>
      getInstances(basePath, id) flatMap { in =>
        if (in.isEmpty)
          throw new IllegalStateException(s"No instances found for $basePath")

        val futures = in.map { i =>
          (cm ? ExecuteRemoteCommand[Input](id, i.getAddress, i.getPort, bean, timeout)) (timeout).mapTo[Output]
        }

        Future.sequence(futures)
      }
    } recover {
      case f: Throwable =>
        throw CommandException("CommandManager", f)
    }
  }
}
