package org.openlaw.scuttlebutt.persistence.driver

import java.util.function.Function

import akka.NotUsed
import akka.actor.{Actor, Stash}
import akka.event.Logging
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import org.apache.tuweni.scuttlebutt.rpc.mux.exceptions.ConnectionClosedException
import org.apache.tuweni.scuttlebutt.rpc.{RPCAsyncRequest, RPCResponse, RPCStreamRequest}
import org.apache.tuweni.scuttlebutt.rpc.mux.{Multiplexer, RPCHandler, ScuttlebuttStreamHandler}
import org.logl.LoggerProvider
import org.openlaw.scuttlebutt.persistence.converters.FutureConverters
import org.openlaw.scuttlebutt.persistence.serialization.ScuttlebuttPersistenceSerializer

import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.pattern.pipe

import scala.concurrent.Future

private case class AttemptConnection()

private case class ReconnectingException() extends ConnectionClosedException()

private case class ConnectionStarted(rpcHandler: RPCHandler)
private case class ConnectionClosed(requestMadeAt: Long)

/**
  * Accepts request messages, and sequences re-connection attempts which the connection is broken
  * (returning connection closed exceptions in the meantime.)
  * @param config
  */
class ScuttlebuttDriverActor(config: Config) extends Actor with Stash {

  val logger = Logging(context.system, this)

  private val system = context.system
  private implicit val executionContext = system.dispatcher

  private var hasTriedInitialConnection: Boolean = false
  private var rpcHandler: Multiplexer = null
  private var lastConnectionReAttempt = 0L

  private def createNewRpcHandler(): Unit = {
    val serializer =  new ScuttlebuttPersistenceSerializer(system)
    val objectMapper = serializer.getObjectMapper()

    val multiplexerLoader =  new MultiplexerLoader(objectMapper, config)

    multiplexerLoader.loadMultiplexerAsync match {
      case Left(err) =>
        logger.error(s"Error with configuration fpr scuttlebutt akka persistence backend: ${err}")
      case Right(handlerAsync) => {
        val futureResult = FutureConverters.asyncResultToFuture(handlerAsync)

        futureResult.onComplete({
          case Success(rpcHandler) => self ! ConnectionStarted(rpcHandler)
          case Failure(exception) => {
            logger.error(s"Failed to connect to scuttlebutt backend. Retrying after 5 seconds. Exception: ${exception}", exception)

            system.scheduler.scheduleOnce(5 seconds) {
              self ! AttemptConnection()
            }
          }
        })
      }
    }

  }

  override def aroundPreStart(): Unit = {
    super.aroundPreStart()
    createNewRpcHandler()
  }

  /*
   * The actor behaviour when the connection is not yet open (or re-opened.)
   */
  override def receive: Receive = {
    case ConnectionStarted(handler) =>
      hasTriedInitialConnection = true
      rpcHandler = handler
      context.become(receiveWhileConnectionOpen)
      unstashAll()

    case AttemptConnection() =>
      hasTriedInitialConnection = true
      createNewRpcHandler()

    case ConnectionClosed(_) => // already attempting to reconnect

    // We stash if the driver has just been started and the initial connection has not yet been attempted.
    case _ if !hasTriedInitialConnection => stash()

    // We may not be able to service the request quickly because the connection is broken and we're trying to re-connect,
    // so we reply with an exception leaving it up to the client to retry if it wants, and allowing the client to signal
    // the health of the connection to SSB is bad to some dashboard / logs
    case _ => sender() ! akka.actor.Status.Failure(ReconnectingException())
  }

  /*
   * The actor behaviour while the connection is open (or has just been discovered to be closed and is transitioning
   * to another behaviour.)
   */
  private def receiveWhileConnectionOpen: Receive = {

    case ConnectionClosed(time) if time > lastConnectionReAttempt =>
      lastConnectionReAttempt = System.currentTimeMillis()
      context.become(receive)
      self ! AttemptConnection()

    case ConnectionClosed(_) =>

    case asyncRequest: RPCAsyncRequest =>
      val time = System.currentTimeMillis()

      val asyncResult = rpcHandler.makeAsyncRequest(asyncRequest)
      val futureResult: Future[RPCResponse] = FutureConverters.asyncResultToFuture(asyncResult)

      futureResult.recover({
        case exception: ConnectionClosedException =>
          self ! ConnectionClosed(time)
          akka.actor.Status.Failure(exception)

        case ex => akka.actor.Status.Failure(ex)
      }).pipeTo(sender())

    case streamRequest: RPCStreamRequest =>
      val time = System.currentTimeMillis()

      try {
        val handler = (streamOpener: Function[Runnable, ScuttlebuttStreamHandler]) =>
          rpcHandler.openStream(streamRequest, streamOpener)
        val graph = new TuweniStreamToAkkaSourceShape(system, handler)
        val source = Source.fromGraph(graph).recover({
          case exception: ConnectionClosedException => {
            // todo: make it possible to inspect whether the connection is open in tuweni with an 'isOpen' method
            // rather intercepting here
            self ! ConnectionClosed(time)
            throw exception
          }
        })

        sender() ! source
      } catch {
        case ex: ConnectionClosedException =>
          // We inform the sender that the connection was closed. We don't retry as we cannot know if the request
          // was successful prior to the connection breaking. We try to re-establish the connection
          self ! ConnectionClosed(time)
          sender() ! akka.actor.Status.Failure(ex)
        case ex: Throwable => sender() ! akka.actor.Status.Failure(ex)
      }

  }

}
