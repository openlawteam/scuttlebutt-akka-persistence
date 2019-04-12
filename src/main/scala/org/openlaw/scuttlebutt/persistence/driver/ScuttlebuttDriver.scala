package org.openlaw.scuttlebutt.persistence.driver

import java.util
import java.util.function.Function

import akka.persistence.PersistentRepr
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import com.google.common.base.Optional
import jdk.internal.org.objectweb.asm.TypeReference
import net.consensys.cava.concurrent.AsyncResult
import net.consensys.cava.scuttlebutt.rpc._
import net.consensys.cava.scuttlebutt.rpc.mux.exceptions.ConnectionClosedException
import net.consensys.cava.scuttlebutt.rpc.mux.{Multiplexer, ScuttlebuttStreamHandler}
import org.openlaw.scuttlebutt.persistence.converters.FutureConverters.asyncResultToFuture

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class ScuttlebuttDriver(multiplexer: Multiplexer, objectMapper: ObjectMapper) {

  def publishPersistentRep(persistentRepr: PersistentRepr): Future[Try[Unit]] = {
    val message = makeRPCMessage(persistentRepr)
    doScuttlebuttPublish(message)
  }

  // TODO: better query representation than an ObjectNode
  def openQueryStream(query: ObjectNode, streamHandler: Function[Runnable, ScuttlebuttStreamHandler]) = {
    val function: RPCFunction = new RPCFunction(util.Arrays.asList("query"), "read")
    val request: RPCStreamRequest = new RPCStreamRequest(function, util.Arrays.asList(query))

    multiplexer.openStream(request, streamHandler)
  }

  def currentPersistenceIds(): Future[Try[List[String]]] = {
    val function: RPCFunction = new RPCFunction(
      util.Arrays.asList("akkaPersistenceIndex"),
      "currentPersistenceIdsAsync")

    val request: RPCAsyncRequest = new RPCAsyncRequest(function, util.Arrays.asList())

    multiplexer.makeAsyncRequest(request).map {
      case rpcMessage if !rpcMessage.isErrorMessage => Success(rpcMessage.asJSON(objectMapper, classOf[List[String]]))
      case rpcMessage => {
        val errorMsg = rpcMessage.getErrorBody(objectMapper).transform(msg => msg.getMessage).or(rpcMessage.asString())
        Failure(new Exception(errorMsg))
      }
    }

  }


  private def makeRPCMessage(persistentRep: PersistentRepr): RPCAsyncRequest = {
    val func: RPCFunction = new RPCFunction("publish")
    val repWithClassName: PersistentRepr = persistentRep.withManifest(persistentRep.payload.getClass.getName)
    val reqBody: ObjectNode = objectMapper.valueToTree(repWithClassName)

    val typeNode = JsonNodeFactory.instance.textNode("akka-persistence-message")

    reqBody.set("type", typeNode)

    new RPCAsyncRequest(func, util.Arrays.asList(reqBody))
  }

  private def doScuttlebuttPublish(request: RPCAsyncRequest): Future[Try[Unit]] = {

    val rpcResult: AsyncResult[RPCMessage] = multiplexer.makeAsyncRequest(request)
    val resultFuture: Future[RPCMessage] = asyncResultToFuture(rpcResult)

    resultFuture.map(
      result => {
        if (result.lastMessageOrError()) {
          val message: Optional[RPCErrorBody] = result.getErrorBody(objectMapper)
          val exception = message.transform(body => new Exception(body.getMessage)).or(new Exception(result.asString()))
          Failure(exception)
        } else {
          Success()
        }
      }
    ).recover({
      // The AsyncWriteJournal interface requires that we only complete the future with an exception if it's
      // a connection break that is the underlying cause, otherwise we return a 'Try' Failure
      case x if !x.isInstanceOf[ConnectionClosedException] => Failure(x)
    })

  }

}
