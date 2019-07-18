package org.openlaw.scuttlebutt.persistence

import java.util.concurrent.{BlockingDeque, BlockingQueue, ConcurrentLinkedQueue, LinkedBlockingDeque}

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic, OutHandler}
import org.apache.tuweni.scuttlebutt.rpc.RPCResponse
import org.apache.tuweni.scuttlebutt.rpc.mux.ScuttlebuttStreamHandler
import org.openlaw.scuttlebutt.persistence.driver.ScuttlebuttDriver

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise, blocking}
import scala.util.Success

object ScuttlebuttEventSource {

  val pool = java.util.concurrent.Executors.newCachedThreadPool()

}

class ScuttlebuttEventSource(
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

      val concurrentQueue = new LinkedBlockingDeque[Option[RPCResponse]] ()

      var pushNewCallback: AsyncCallback[RPCResponse] = null;

      override def preStart(): Unit = {
        // "push" cannot be called in another thread, but we can use this mechanism
        // to invoke it on the appropriate thread from another thread
        pushNewCallback = getAsyncCallback(item => push(out, item))
      }

      override def postStop(): Unit = {
        // If the downstream stopped consuming elements, we need to close the
        // scuttlebutt stream - however, the 'stopper' is in another thread so we use
        // a promise to future to close it in a thread safe way from the stream
        // consuming thread
        downStreamEndedPromise.complete(Success(true))
      }

      scuttlebuttDriver.eventsByPersistenceId(author, persistenceId, start, end, live,
        (streamStopper) => new ScuttlebuttStreamHandler() {

          downStreamEnded.foreach(_ => streamStopper.run())

          override def onMessage(message: RPCResponse): Unit = {
            concurrentQueue.put(Some(message))
          }

          override def onStreamEnd(): Unit = {
            // Signal that there will be no more elements by populating the queue with "None"
            // so that the source can be closed
            concurrentQueue.put(None)
          }

          override def onStreamError(ex: Exception): Unit = {
            fail(out, ex)
          }
        })

      setHandler(out, new OutHandler {

        override def onPull(): Unit = {

          ScuttlebuttEventSource.pool.submit(new Runnable() {

            override def run(): Unit = {
              // Wait for a new item to become available over the stream before pushing it
              val item = concurrentQueue.take()

              item match {
                case None => complete(out)
                case Some(streamItem) => pushNewCallback.invoke(streamItem)
              }}

          })

        }

      })

    }
  }

  override def shape: SourceShape[RPCResponse] = SourceShape(out)
}
