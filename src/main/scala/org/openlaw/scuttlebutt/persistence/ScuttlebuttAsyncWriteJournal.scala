package org.openlaw.scuttlebutt.persistence


import java.util

import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.Config
import net.consensys.cava.concurrent.AsyncResult
import net.consensys.cava.scuttlebutt.rpc.mux.exceptions.ConnectionClosedException
import net.consensys.cava.scuttlebutt.rpc.mux.{RPCHandler, ScuttlebuttStreamHandler}
import net.consensys.cava.scuttlebutt.rpc._

import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, _}
import scala.util.{Failure, Success, Try}
import FutureConverters.asyncResultToFuture
import com.google.common.base.Optional


class ScuttlebuttAsyncWriteJournal(config: Config) extends AsyncWriteJournal {

  val objectMapper: ObjectMapper = new ScuttlebuttPersistenceSerializationConfig().getObjectMapper()
  val loader: MultiplexerLoader = new MultiplexerLoader(objectMapper, config)

  val rpcHandler: RPCHandler = loader.loadMultiplexer

  def makeRPCMessage(persistentRep: PersistentRepr): RPCAsyncRequest = {
    val func: RPCFunction = new RPCFunction("publish")
    val repWithClassName: PersistentRepr = persistentRep.withManifest(persistentRep.payload.getClass.getName)
    val reqBody: ObjectNode = objectMapper.valueToTree(repWithClassName)
    reqBody.set("type", reqBody.get("persistenceId"))

    new RPCAsyncRequest(func, util.Arrays.asList(reqBody))
  }

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {

    if (containsUnsupportedBulkWrite(messages)) {
      Future.failed(new UnsupportedOperationException("Scuttlebutt backend does not currently support atomic bulk writes."))
    } else {
      val individualMessages = messages.map(_.payload).flatten.sortBy(_.sequenceNr)
      val reqs: immutable.Seq[RPCAsyncRequest] = individualMessages.map(makeRPCMessage)
      val emptySeq: immutable.Seq[Try[Unit]] = immutable.Seq()

      // We perform each write sequentially, and if any of the previous writes fail we do not attempt any
      // subsequent writes
      reqs.foldRight[Future[immutable.Seq[Try[Unit]]]](Future.successful(emptySeq)) {
        (req: RPCAsyncRequest, result: Future[immutable.Seq[Try[Unit]]]) => {
          result.flatMap(statuses => {
            statuses.find(result => result.isFailure) match {
              case Some(x) => Future.successful(statuses :+ Failure(new Exception("Previous write failed")))
              case None => {
                // publish and add to the list of results we're building
                doScuttlebuttPublish(req).map(result => statuses :+ result)
              }
            }
          })
        }
      }
    }
  }

  private def doScuttlebuttPublish(request: RPCAsyncRequest): Future[Try[Unit]] = {

    val rpcResult: AsyncResult[RPCMessage] = rpcHandler.makeAsyncRequest(request)
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

  def containsUnsupportedBulkWrite(messages: immutable.Seq[AtomicWrite]): Boolean = {
    return messages.find(msg => msg.payload.length > 1).isDefined
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long) =
    throw new UnsupportedOperationException("Deletions are not yet supported.")

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
    recoveryCallback: PersistentRepr => Unit
  ) = {
    val finishedReplaysPromise = Promise[Unit]();
    var function: RPCFunction = new RPCFunction(util.Arrays.asList("query"), "read")
    val query = makeReplayQuery(persistenceId, fromSequenceNr, Some(toSequenceNr), max)
    val request: RPCStreamRequest = new RPCStreamRequest(function, util.Arrays.asList(query))

    rpcHandler.openStream(request, (closer: Runnable) => {
      new PersistentReprStreamHandler(objectMapper, closer, recoveryCallback, finishedReplaysPromise)
    })

    finishedReplaysPromise.future
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    val result = Promise[Long]()
    val query = makeReplayQuery(persistenceId, fromSequenceNr, None, 1, reverse = true)
    var function: RPCFunction = new RPCFunction(util.Arrays.asList("query"), "read")
    val req: RPCStreamRequest = new RPCStreamRequest(function, util.Arrays.asList(query))

    rpcHandler.openStream(req, (closer: Runnable) => {
      new ScuttlebuttStreamHandler() {
        override def onMessage(rpcMessage: RPCMessage): Unit = {
          val node: ObjectNode = rpcMessage.asJSON(objectMapper, classOf[ObjectNode])
          val content: JsonNode = node.findPath("content")

          val responseBody: PersistentRepr = objectMapper.treeToValue(content, classOf[PersistedMessage])
          result.success(responseBody.sequenceNr)
        }

        override def onStreamEnd(): Unit = {
          if (!result.isCompleted) {
            result.success(0)
          }
        }

        override def onStreamError(e: Exception): Unit = {
          result.failure(e)
        }
      }
    })

    return result.future
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
}