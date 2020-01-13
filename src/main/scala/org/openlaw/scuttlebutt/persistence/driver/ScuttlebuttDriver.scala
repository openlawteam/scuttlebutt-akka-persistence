package org.openlaw.scuttlebutt.persistence.driver

import java.io.ByteArrayInputStream
import java.util
import java.util.function.Function

import akka.NotUsed
import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.tuweni.scuttlebutt.rpc._
import org.apache.tuweni.scuttlebutt.rpc.mux.exceptions.ConnectionClosedException
import org.apache.tuweni.scuttlebutt.rpc.mux.ScuttlebuttStreamHandler
import org.openlaw.scuttlebutt.persistence.model.{AuthorStreamOptions, StreamOptions, WhoAmIResponse}
import org.openlaw.scuttlebutt.persistence.serialization.{PersistedMessage, ScuttlebuttPersistenceSerializer}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class ScuttlebuttDriver(
                         actorSystem: ActorSystem,
                         multiplexer: ReconnectingScuttlebuttConnection,
                         objectMapper: ObjectMapper,
                         scuttlebuttPersistenceSerializer: ScuttlebuttPersistenceSerializer
                       ) {

  implicit val materializer = ActorMaterializer()(actorSystem)

  def getLiveAuthorStream(): Source[String, NotUsed] = {
    val function: RPCFunction = new RPCFunction(
      util.Arrays.asList("akkaPersistenceIndex", "persistenceIds"),
      "allOtherAuthors")

    val request: RPCStreamRequest = new RPCStreamRequest(function, util.Arrays.asList(new AuthorStreamOptions(true)))

    multiplexer.openStream(request)
      .map(response => response.asString())
  }

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
                              live: Boolean): Source[RPCResponse, NotUsed] = {


    // 'null' for the author field is a shortcut for 'my ident'.
    eventsByPersistenceId(null, persistenceId, fromSequenceNr, toSequenceNr, live)
  }

  def eventsByPersistenceId(author: String,
                            persistenceId: String,
                            fromSequenceNr: Long,
                            toSequenceNr: Long,
                            live: Boolean): Source[RPCResponse, NotUsed] = {

    val function: RPCFunction = new RPCFunction(
      util.Arrays.asList("akkaPersistenceIndex", "events"),
      "eventsByPersistenceId")

    val request = new RPCStreamRequest(function, util.Arrays.asList(
       author.asInstanceOf[Object],
      persistenceId.asInstanceOf[Object],
      fromSequenceNr.asInstanceOf[Object],
      toSequenceNr.asInstanceOf[Object],
      live.asInstanceOf[Object]))

    multiplexer.openStream(request)
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

    multiplexer.makeAsyncRequest(request).map(response => response.asJSON(
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

  /**
    * Makes an RPC stream request, and fills each item in the stream response into a list of strings.
    * Assumes that the RPC response body type is a 'string' (not JSON, etc.)
    *
    * @param request the RPC async request details.
    * @return The future is completed with a result list if successful, otherwise it is completed with a failure
    */
  private def stringStreamToArrayHandler(request: RPCStreamRequest): Future[Try[List[String]]] = {
    rpcResponseArrayFiller(request).map(_.map(_.map(_.asString())))
  }

  def getEventsForAuthor(authorId: String, start: Long, end: Long): Future[Try[List[RPCResponse]]] = {
    val function: RPCFunction = new RPCFunction(
      util.Arrays.asList("akkaPersistenceIndex", "events"),
      "allEventsForAuthor")

    val options: StreamOptions = new StreamOptions(start, end, false)
    val request: RPCStreamRequest = new RPCStreamRequest(function, util.Arrays.asList(authorId, options))

    rpcResponseArrayFiller(request)
  }

  /**
    * Opens a scuttlebutt stream request, and fills the stream items into a list of RPCResponse objects. Each item
    * may contain strings, binary, JSON, etc. A higher level function should marshall this data.
    *
    * @param request the request details
    * @return the list of RPC responses for each stream item if successful, otherwise an error
    */
  private def rpcResponseArrayFiller(request: RPCStreamRequest): Future[Try[List[RPCResponse]]] = {
    val result = multiplexer.openStream(request).toMat(Sink.seq)(Keep.right).run().map(_.toList)

    result.map(Success(_)).recover {
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

    val resultFuture: Future[RPCResponse] = multiplexer.makeAsyncRequest(request)

    // Success if the future wasn't failed
    resultFuture.map(_ => Success()).recover({
      // The AsyncWriteJournal interface requires that we only complete the future with an exception if it's
      // a connection break that is the underlying cause, otherwise we return a 'Try' Failure.
      case x if !x.isInstanceOf[ConnectionClosedException] => Failure(x)
    })

  }

}
