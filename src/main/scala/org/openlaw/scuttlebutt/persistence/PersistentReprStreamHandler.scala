package org.openlaw.scuttlebutt.persistence

import akka.persistence.PersistentRepr
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import net.consensys.cava.scuttlebutt.rpc.{RPCMessage, RPCResponse}
import net.consensys.cava.scuttlebutt.rpc.mux.ScuttlebuttStreamHandler
import org.openlaw.scuttlebutt.persistence.serialization.PersistedMessage

import scala.concurrent.Promise

class PersistentReprStreamHandler(
                                   objectMapper: ObjectMapper,
                                   closer: Runnable,
                                   recoveryCallback: PersistentRepr => Unit,
                                   finishedPromise: Promise[Unit])
  extends ScuttlebuttStreamHandler {

  override def onMessage(rpcMessage: RPCResponse): Unit = {
    val node:ObjectNode = rpcMessage.asJSON(objectMapper, classOf[ObjectNode])
    val content: JsonNode = node.findPath("content")
    val payload: JsonNode = content.findPath("payload")

    val persistentRepr : PersistentRepr = objectMapper.treeToValue(content, classOf[PersistedMessage])

    recoveryCallback(persistentRepr.withPayload(payload))
  }

  override def onStreamEnd(): Unit = {
    finishedPromise.success()
  }

  override def onStreamError(e: Exception): Unit = {
    finishedPromise.failure(e)
  }
}

