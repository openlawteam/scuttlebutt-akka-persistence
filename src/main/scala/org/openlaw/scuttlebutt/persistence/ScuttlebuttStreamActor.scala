package org.openlaw.scuttlebutt.persistence

import akka.actor.Actor
import akka.stream.stage.AsyncCallback
import org.apache.tuweni.scuttlebutt.rpc.RPCResponse

case class ReadyForNext()
case class StreamEnd()

import scala.collection.immutable.Queue

class ScuttlebuttStreamActor(
                             asyncCallback: AsyncCallback[RPCResponse],
                             completeStream: AsyncCallback[Unit]
                           ) extends Actor {

  var streamEnded = false
  var isReadyToReceive = false
  var queue = Queue[RPCResponse]()

  override def receive: Receive = {

    case StreamEnd() => {
      streamEnded = true

      endStreamIfAppropriate()
    }
    case response: RPCResponse if isReadyToReceive => {
      isReadyToReceive = false
      asyncCallback.invoke(response)
    }
    case response: RPCResponse if !isReadyToReceive => {
      queue = queue.enqueue(response)
    }
    case ReadyForNext() => {
      if (queue.isEmpty) {
        isReadyToReceive = true
      } else {
        val (response, newQueue) = queue.dequeue
        queue = newQueue
        asyncCallback.invoke(response)

        isReadyToReceive = false
      }

      endStreamIfAppropriate()
    }
  }

  private def endStreamIfAppropriate(): Unit = {
    if (queue.isEmpty && streamEnded) {
      completeStream.invoke()
    }
  }

}
