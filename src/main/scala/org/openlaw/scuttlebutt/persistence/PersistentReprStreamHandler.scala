package org.openlaw.scuttlebutt.persistence

import akka.persistence.PersistentRepr
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import net.consensys.cava.scuttlebutt.rpc.RPCMessage
import net.consensys.cava.scuttlebutt.rpc.mux.ScuttlebuttStreamHandler

import scala.concurrent.Promise

class PersistentReprStreamHandler(
                                   objectMapper: ObjectMapper,
                                   closer: Runnable,
                                   recoveryCallback: PersistentRepr => Unit,
                                   finishedPromise: Promise[Unit])
  extends ScuttlebuttStreamHandler {

  override def onMessage(rpcMessage: RPCMessage): Unit = {
    val node:ObjectNode = rpcMessage.asJSON(objectMapper, classOf[ObjectNode])
    val content: JsonNode = node.findPath("content")
    val payload = content.findPath("payload")

    val persistentRepr : PersistentRepr = objectMapper.treeToValue(content, classOf[PersistedMessage])

    // TODO: do we need some sort of pluggable serializer in case classes move to different packages, etc?

    val targetClass: String = persistentRepr.manifest
    val clazz: Class[_]  = Class.forName(targetClass)

    // We deserialize the payload into the class in the manifest (that it was written to JSON from.)
    val deserializedPayload = objectMapper.readValue(payload.toString(), clazz)
    val reprWithTypedPayload = persistentRepr.withPayload(deserializedPayload)

    recoveryCallback(reprWithTypedPayload)
  }

  override def onStreamEnd(): Unit = {
    finishedPromise.success()
  }

  override def onStreamError(e: Exception): Unit = {
    throw e
  }
}

