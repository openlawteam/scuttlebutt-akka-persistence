package org.openlaw.scuttlebutt.persistence

import java.util

import akka.persistence.PersistentRepr
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.common.base.Optional
import net.consensys.cava.concurrent.AsyncResult
import net.consensys.cava.scuttlebutt.rpc.{RPCAsyncRequest, RPCErrorBody, RPCFunction, RPCMessage}
import net.consensys.cava.scuttlebutt.rpc.mux.Multiplexer
import net.consensys.cava.scuttlebutt.rpc.mux.exceptions.ConnectionClosedException
import org.openlaw.scuttlebutt.persistence.FutureConverters.asyncResultToFuture

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

import scala.concurrent.ExecutionContext.Implicits.global

class ScuttlebuttDriver(multiplexer: Multiplexer, objectMapper: ObjectMapper) {

  def makeReplayQuery(
                       persistenceId: String,
                       fromSequenceNr: Long,
                       max: Long,
                       reverse: Boolean): ObjectNode = {

    makeReplayQuery(persistenceId, fromSequenceNr, None, max, reverse)
  }

  def makeReplayQuery(
                       persistenceId: String,
                       fromSequenceNr: Long,
                       toSequenceNr: Long,
                       max: Long,
                       reverse: Boolean): ObjectNode = {

    makeReplayQuery(persistenceId, fromSequenceNr, Some(toSequenceNr), max, reverse)
  }

  private def makeReplayQuery(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Option[Long], max: Long, reverse: Boolean = false): ObjectNode = {
    val rangeFilter = Map("$gte" -> fromSequenceNr) ++ toSequenceNr.fold(Map[String, Long]())(to => Map("$lte" -> to))

    // TODO: filter to just own public key as author. Also, make a query builder class and class representation of a query
    val query = Map(
      "query" ->
        List(
          Map("$filter" ->
            Map("value" ->
              Map("content" -> Map(
                ("type" -> persistenceId),
                ("sequenceNr" -> rangeFilter)
              )
              ))
          ),
          Map("$sort" -> "sequenceNr")
        ),
      "limit" -> max,
      "reverse" -> reverse)

    objectMapper.valueToTree(query)
  }

  def publishPersistentRep(persistentRepr: PersistentRepr): Future[Try[Unit]] = {
    val message = makeRPCMessage(persistentRepr)
    doScuttlebuttPublish(message)
  }

  def makeRPCMessage(persistentRep: PersistentRepr): RPCAsyncRequest = {
    val func: RPCFunction = new RPCFunction("publish")
    val repWithClassName: PersistentRepr = persistentRep.withManifest(persistentRep.payload.getClass.getName)
    val reqBody: ObjectNode = objectMapper.valueToTree(repWithClassName)
    reqBody.set("type", reqBody.get("persistenceId"))

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
