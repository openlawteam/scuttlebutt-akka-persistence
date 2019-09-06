package org.openlaw.scuttlebutt.persistence.driver

import java.io.ByteArrayInputStream
import java.util
import java.util.function.Function

import akka.persistence.PersistentRepr
import akka.persistence.query.{EventEnvelope, Sequence}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import org.apache.tuweni.concurrent.AsyncResult
import org.apache.tuweni.scuttlebutt.rpc._
import org.apache.tuweni.scuttlebutt.rpc.mux.exceptions.ConnectionClosedException
import org.apache.tuweni.scuttlebutt.rpc.mux.{Multiplexer, ScuttlebuttStreamHandler}
import org.openlaw.scuttlebutt.persistence.converters.FutureConverters
import org.openlaw.scuttlebutt.persistence.converters.FutureConverters.asyncResultToFuture
import org.openlaw.scuttlebutt.persistence.model.{StreamOptions, WhoAmIResponse}
import org.openlaw.scuttlebutt.persistence.serialization.{PersistedMessage, ScuttlebuttPersistenceSerializer}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class ScuttlebuttDriver(
                         multiplexer: Multiplexer,
                         objectMapper: ObjectMapper,
                         scuttlebuttPersistenceSerializer: ScuttlebuttPersistenceSerializer
                       ) {

  def publishPersistentRep(persistentRepr: PersistentRepr): Future[Try[Unit]] = {
    val message = makeRPCMessage(persistentRepr)
    doScuttlebuttPublish(message)
  }

  def getHighestSequenceNr(persistenceId: String): Future[Long] = {
    val function: RPCFunction = new RPCFunction(util.Arrays.asList("akkaPersistenceIndex", "events"), "highestSequenceNumber")
    val request: RPCAsyncRequest = new RPCAsyncRequest(function, util.Arrays.asList(null, persistenceId))

    val response: Future[RPCResponse] = multiplexer.makeAsyncRequest(request)

    response.map(result => {
      result.asJSON[Long](objectMapper, classOf[Long])
    })
  }

  def myEventsByPersistenceId(persistenceId: String,
                              fromSequenceNr: Long,
                              toSequenceNr: Long,
                              live: Boolean,
                              handler: Function[Runnable, ScuttlebuttStreamHandler]) = {


    // 'null' for the author field is a shortcut for 'my ident'.
    eventsByPersistenceId(null, persistenceId, fromSequenceNr, toSequenceNr, live, handler)
  }

  def eventsByPersistenceId(author: String,
                            persistenceId: String,
                            fromSequenceNr: Long,
                            toSequenceNr: Long,
                            live: Boolean,
                            handler: Function[Runnable, ScuttlebuttStreamHandler]) = {

    val function: RPCFunction = new RPCFunction(
      util.Arrays.asList("akkaPersistenceIndex", "events"),
      "eventsByPersistenceId")

    val request = new RPCStreamRequest(function, util.Arrays.asList(
       author.asInstanceOf[Object],
      persistenceId.asInstanceOf[Object],
      fromSequenceNr.asInstanceOf[Object],
      toSequenceNr.asInstanceOf[Object],
      live.asInstanceOf[Object]))

    multiplexer.openStream(request, handler)
  }

  def currentPersistenceIds(): Future[Try[List[String]]] = {
    val function: RPCFunction = new RPCFunction(
      util.Arrays.asList("akkaPersistenceIndex", "persistenceIds"),
      "myCurrentPersistenceIdsAsync")

    val request: RPCAsyncRequest = new RPCAsyncRequest(function, util.Arrays.asList())

    multiplexer.makeAsyncRequest(request).map(result => Success(result.asJSON(objectMapper, classOf[List[String]]) )).recover {
      case exception => Failure(exception)
    }
  }

  def getPersistenceIdsForAuthor(authorId: String, start: Long, end: Long, reverse: Boolean = false): Future[Try[List[String]]] = {
    val function: RPCFunction = new RPCFunction(
      util.Arrays.asList("akkaPersistenceIndex", "persistenceIds"),
      "persistenceIdsForAuthor")

    val options: StreamOptions = new StreamOptions(start, end, reverse)
    val request: RPCStreamRequest = new RPCStreamRequest(function, util.Arrays.asList(authorId, options))

    stringStreamToArrayHandler(request)
  }

  def getAuthorsForPersistenceId(persistenceId: String): Future[Try[List[String]]] = {
    val function: RPCFunction = new RPCFunction(
      util.Arrays.asList("akkaPersistenceIndex", "persistenceIds"),
      "authorsForPersistenceId")

    val request: RPCStreamRequest = new RPCStreamRequest(function, util.Arrays.asList(persistenceId))

    stringStreamToArrayHandler(request)
  }

  /**
    * @return the public key of the instance
    */
  def getMyIdentity(): Future[Try[String]] = {
    val function: RPCFunction = new RPCFunction("whoami")

    val request: RPCAsyncRequest = new RPCAsyncRequest(function, util.Arrays.asList())

    asyncResultToFuture(multiplexer.makeAsyncRequest(request)).map(response => response.asJSON(
      objectMapper, classOf[WhoAmIResponse]
    )).map(id => Success(id.id)).recover{
      case ex: Throwable => Failure(ex)
    }
  }

  def getAllAuthors(): Future[Try[List[String]]] = {
    val function: RPCFunction = new RPCFunction(
      util.Arrays.asList("akkaPersistenceIndex", "persistenceIds"),
      "allOtherAuthors")

    val request: RPCStreamRequest = new RPCStreamRequest(function, util.Arrays.asList())

    stringStreamToArrayHandler(request)
  }

  private def stringStreamToArrayHandler(request: RPCStreamRequest) = {
    val promise: Promise[List[String]] = Promise()

    multiplexer.openStream(request, (stopper) => {
      new ScuttlebuttStreamHandler {
        var results: Seq[String] = List()

        override def onMessage(message: RPCResponse): Unit = {
          results = results :+ message.asString()
        }

        override def onStreamEnd(): Unit = {
          promise.success(results.toList)
        }

        override def onStreamError(ex: Exception): Unit = {
          promise.failure(ex)
        }
      }
    })

    promise.future.map(Success(_)).recover {
      case exception => Failure(exception)
    }
  }

  def getEventsForAuthor(authorId: String, start: Long, end: Long): Future[Try[List[RPCResponse]]] = {
    val function: RPCFunction = new RPCFunction(
      util.Arrays.asList("akkaPersistenceIndex", "events"),
      "allEventsForAuthor")

    val options: StreamOptions = new StreamOptions(start, end, false)
    val request: RPCStreamRequest = new RPCStreamRequest(function, util.Arrays.asList(authorId, options))

    rpcResponseArrayFiller(request)
  }

  private def rpcResponseArrayFiller(request: RPCStreamRequest) = {
    var promise: Promise[List[RPCResponse]] = Promise()

    multiplexer.openStream(request, (stopper) => {
      new ScuttlebuttStreamHandler {
        var results: Seq[RPCResponse] = List()

        override def onMessage(message: RPCResponse): Unit = {
          results = results :+ message
        }

        override def onStreamEnd(): Unit = {
          promise.success(results.toList)
        }

        override def onStreamError(ex: Exception): Unit = {
          promise.failure(ex)
        }
      }
    })

    promise.future.map(Success(_)).recover {
      case exception => Failure(exception)
    }
  }


  private def makeRPCMessage(persistentRep: PersistentRepr): RPCAsyncRequest = {

    val payload = persistentRep.payload.asInstanceOf[AnyRef]
    val bytes = scuttlebuttPersistenceSerializer.serialize(payload).get
    val tree = objectMapper.reader().readTree(new ByteArrayInputStream(bytes))

    val func: RPCFunction = new RPCFunction(util.Arrays.asList("akkaPersistenceIndex", "events"), "persistEvent")

    val repWithClassName: PersistentRepr = persistentRep.withManifest(persistentRep.payload.getClass.getName)
    val withPayload = repWithClassName.withPayload(tree)

    val reqBody = PersistedMessage(
      withPayload.payload,
      withPayload.manifest,
      withPayload.persistenceId,
      withPayload.sequenceNr,
      withPayload.writerUuid,
      withPayload.deleted,
      withPayload.sender
    )

    new RPCAsyncRequest(func, util.Arrays.asList(reqBody))
  }

  private def doScuttlebuttPublish(request: RPCAsyncRequest): Future[Try[Unit]] = {

    val rpcResult: AsyncResult[RPCResponse] = multiplexer.makeAsyncRequest(request)
    val resultFuture: Future[RPCResponse] = asyncResultToFuture(rpcResult)

    // Success if the future wasn't failed
    resultFuture.map(_ => Success()).recover({
      // The AsyncWriteJournal interface requires that we only complete the future with an exception if it's
      // a connection break that is the underlying cause, otherwise we return a 'Try' Failure.
      case x if !x.isInstanceOf[ConnectionClosedException] => Failure(x)
    })

  }

}
