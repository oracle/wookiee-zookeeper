package com.webtrends.harness.component.zookeeper

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.webtrends.harness.component.Component
import com.webtrends.harness.component.zookeeper.discoverable.{DiscoverableCommandExecution, DiscoverableCommandHelper}
import com.webtrends.harness.service.test.BaseWookieeTest
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class DiscoverableCommandSpec extends BaseWookieeTest with WordSpecLike with MustMatchers with BeforeAndAfterAll with PatienceConfiguration {
  val path = "/discovery/test"
  val testServiceName = "DiscoveryService"
  implicit val to: Timeout = Timeout(120.seconds)
  var commandActor: ActorRef = _
  eventually(timeout(5.seconds), interval(200.milliseconds)) { ZookeeperService.getMediator(system) must not be None }

  override def componentMap: Option[Map[String, Class[_ <: Component]]] = Some(Map("wookiee-zookeeper" -> classOf[ZookeeperManager]))

  override def config: Config = {
    ConfigFactory.parseString(s"""
      wookiee-zookeeper {
        enabled = true
        mock-enabled = true
        mock-port = 59595
        base-path = "/discovery/test"
        register-self = false
      }
      akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      """.stripMargin
    )
  }

  "Discoverable Commands" should {
    case class Input(str: String)
    case class Output(str: String)
    case class AddCommand(id: String, logic: Input => Future[Output])
    case class ExecuteCommand(id: String, input: Input)

    "Be able to add and execute a new Command" in {
      commandActor = system.actorOf(Props(new Actor with DiscoverableCommandHelper with DiscoverableCommandExecution {
        import context.dispatcher

        def receive: Receive = {
          case add: AddCommand =>
            val send = sender()
            addDiscoverableEndpoint(add.id, add.logic) recover {
              case ex: Throwable =>
                ex.printStackTrace()
            } foreach { _ =>
              send ! true
            }
          case exec: ExecuteCommand =>
            val send = sender()
            executeDiscoverableCommand[Input, Output](path, exec.id, exec.input) recover {
              case ex: Throwable =>
                ex.printStackTrace()
            } foreach { output =>
              send ! output
            }
        }

        override implicit val basePath: String = path
      }), "TestActor")

      val create = Await.result((commandActor ? AddCommand("test-id", { str: Input =>
        Future.successful(Output(str.str + "-Output"))
      })).mapTo[Boolean], to.duration)
      create mustEqual true

      val result = Await.result((commandActor ? ExecuteCommand("test-id", Input("Input"))).mapTo[Output], to.duration)
      result mustEqual Output("Input-Output")
    }
  }

  override protected def afterAll(): Unit =
    shutdown()
}
