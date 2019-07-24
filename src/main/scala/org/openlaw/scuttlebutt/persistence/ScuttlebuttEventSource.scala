package org.openlaw.scuttlebutt.persistence

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic, OutHandler}
import org.apache.tuweni.scuttlebutt.rpc.RPCResponse
import org.apache.tuweni.scuttlebutt.rpc.mux.ScuttlebuttStreamHandler
import org.openlaw.scuttlebutt.persistence.driver.ScuttlebuttDriver

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise
import scala.util.Success

class ScuttlebuttEventSource(
                               actorSystem: ActorSystem,
                               scuttlebuttDriver: ScuttlebuttDriver,
                               persistenceId: String,
                               start: Long,
                               end: Long,
                               author: String,
                               live: Boolean) extends GraphStage[SourceShape[RPCResponse]] {

  val out: Outlet[RPCResponse] = Outlet("ScuttlebuttEventSource")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) {

      val downStreamEndedPromise = Promise[Boolean]()
      val downStreamEnded = downStreamEndedPromise.future

      var actor: ActorRef = null;

      override def preStart(): Unit = {
        // "push" cannot be called in another thread, but we can use this mechanism
        // to invoke it on the appropriate thread from another thread
        val pushNewCallback = getAsyncCallback[RPCResponse](item => push(out, item))
        val completeStream = getAsyncCallback[Unit]((_) => complete(out))
        val failStream = getAsyncCallback[Exception](ex => fail(out, ex))

        actor = actorSystem.actorOf(Props(classOf[ScuttlebuttEventActor],  pushNewCallback, completeStream))

        scuttlebuttDriver.eventsByPersistenceId(author, persistenceId, start, end, live,
          (streamStopper) => new ScuttlebuttStreamHandler() {

            downStreamEnded.foreach(_ => streamStopper.run())

            override def onMessage(message: RPCResponse): Unit = {
              actor ! message
            }

            override def onStreamEnd(): Unit = {
              // Signal that there will be no more elements by populating the queue with "None"
              // so that the source can be closed

              actor ! StreamEnd()
            }

            override def onStreamError(ex: Exception): Unit = {
              failStream.invoke(ex)
            }
          })
      }

      override def postStop(): Unit = {
        // If the downstream stopped consuming elements, we need to close the
        // scuttlebutt stream - however, the 'stopper' is in another thread so we use
        // a promise to future to close it in a thread safe way from the stream
        // consuming thread
        actor ! PoisonPill
        downStreamEndedPromise.complete(Success(true))
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          actor ! ReadyForNext()
        }
      })

    }
  }

  override def shape: SourceShape[RPCResponse] = SourceShape(out)
}
