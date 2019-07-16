package org.openlaw.scuttlebutt.persistence

import akka.persistence.PersistentRepr
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.tuweni.scuttlebutt.rpc.{RPCMessage, RPCResponse}
import org.apache.tuweni.scuttlebutt.rpc.mux.ScuttlebuttStreamHandler
import org.openlaw.scuttlebutt.persistence.serialization.{PersistedMessage, ScuttlebuttPersistenceSerializer}

import scala.concurrent.Promise
import scala.util.{Failure, Success}

class PersistentReprStreamHandler(
                                   val objectMapper: ObjectMapper,
                                   scuttlebuttPersistenceSerializer: ScuttlebuttPersistenceSerializer,
                                   closer: Runnable,
                                   recoveryCallback: PersistentRepr => Unit,
                                   finishedPromise: Promise[Unit])
  extends ScuttlebuttStreamHandler {

  override def onMessage(rpcMessage: RPCResponse): Unit = {
    val content:ObjectNode = rpcMessage.asJSON(objectMapper, classOf[ObjectNode])
    val payload: JsonNode = content.findPath("payload")
    val payloadBytes = objectMapper.writeValueAsBytes(payload)

    val persistentRepr : PersistentRepr = objectMapper.treeToValue(content, classOf[PersistedMessage])
    val deserializePayload = scuttlebuttPersistenceSerializer.deserialize(payloadBytes,  persistentRepr.manifest)

    deserializePayload match {
      case Success(value) => recoveryCallback(persistentRepr.withPayload(value))
      case Failure(ex) => {
        finishedPromise.failure(ex)
        closer.run()
      }
    }

  }

  override def onStreamEnd(): Unit = {
    if (!finishedPromise.isCompleted) finishedPromise.success()
  }

  override def onStreamError(e: Exception): Unit = {
    finishedPromise.failure(e)
  }
}

