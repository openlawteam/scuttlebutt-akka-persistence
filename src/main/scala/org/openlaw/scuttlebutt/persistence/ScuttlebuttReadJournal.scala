package org.openlaw.scuttlebutt.persistence

import java.util.function.Function

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.persistence.{Persistence, PersistentRepr}
import akka.persistence.query.{EventEnvelope, Offset, Sequence}
import akka.persistence.query.scaladsl.ReadJournal
import akka.stream.scaladsl.Source
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.config.Config
import org.apache.tuweni.scuttlebutt.rpc.RPCResponse
import org.apache.tuweni.scuttlebutt.rpc.mux.ScuttlebuttStreamHandler
import org.openlaw.scuttlebutt.persistence.driver.{ScuttlebuttDriver, TuweniStreamToAkkaSourceShape}
import org.openlaw.scuttlebutt.persistence.model.AllowAccess
import org.openlaw.scuttlebutt.persistence.reader.{PageStream, ScuttlebuttStreamRangeFiller}
import org.openlaw.scuttlebutt.persistence.serialization.{PersistedMessage, ScuttlebuttPersistenceSerializer}

import scala.concurrent.Future
import scala.util.Try
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.blocking


class ScuttlebuttReadJournal(
                              system: ExtendedActorSystem,
                              config: Config,
                              scuttlebuttDriver: ScuttlebuttDriver,
                              objectMapper: ObjectMapper,
                              scuttlebuttPersistenceSerializer: ScuttlebuttPersistenceSerializer) extends ReadJournal
  with akka.persistence.query.scaladsl.EventsByTagQuery
  with akka.persistence.query.scaladsl.EventsByPersistenceIdQuery
  with akka.persistence.query.scaladsl.CurrentEventsByPersistenceIdQuery
  with akka.persistence.query.scaladsl.PersistenceIdsQuery
  with akka.persistence.query.scaladsl.CurrentPersistenceIdsQuery {

  val rangeFiller = new ScuttlebuttStreamRangeFiller(system, scuttlebuttDriver, objectMapper)

  /**
    * A stream of the current events for the local instance by persistenceId (stream ends when there
    * are no more elements.)
    *
    * @param persistenceId  the persistence ID to get the events for
    * @param fromSequenceNr the sequence number to start getting events from
    * @param toSequenceNr   the maximum number to get events for (stream will end early if the current largest
    *                       sequence number is larger.)
    * @return A stream of the current persistence IDs for the local instance
    */
  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    eventsByPersistenceIdSource(persistenceId, fromSequenceNr, toSequenceNr, false)
  }

  /**
    * A stream of the current events for the local instance by persistenceId. If there is not yet an event with the
    * 'toSequenceNr' the stream will remain open until there is. This means that a 'live' stream can be opened by
    * using Long.MaxValue for the 'toSequenceNr' parameter and events will be emitted as they are available.
    *
    * @param persistenceId  the persistence ID to get events for
    * @param fromSequenceNr the sequence number to start getting events from
    * @param toSequenceNr   the maximum number to get events for (stream will not end until this has been reached.)
    * @return
    */
  override def eventsByPersistenceId(
                                      persistenceId: String, fromSequenceNr: Long,
                                      toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    eventsByPersistenceIdSource(persistenceId, fromSequenceNr, toSequenceNr, true)
  }

  /**
    * A stream of the current events for the local instance by persistenceId and authorId (stream ends when there
    * are no more elements.)
    *
    * @param author         the author ID for the instance.
    * @param persistenceId  the persistence ID to get the events for
    * @param fromSequenceNr the sequence number to start getting events from
    * @param toSequenceNr   the maximum number to get events for (stream will end early if the current largest
    *                       sequence number is larger.)
    * @return A stream of the current persistence IDs for the local instance
    */
  def currentEventsByAuthorAndPeristenceId(
                                            author: String, persistenceId: String, fromSequenceNr: Long,
                                            toSequenceNr: Long
                                          ): Source[EventEnvelope, NotUsed] = {

    eventsByPersistenceIdSource(persistenceId, fromSequenceNr, toSequenceNr, false, author)
  }

  /**
    * A stream of the current events for the local instance by persistenceId and instance author ID. If there is not yet an event with the
    * 'toSequenceNr' the stream will remain open until there is. This means that a 'live' stream can be opened by
    * using Long.MaxValue for the 'toSequenceNr' parameter and events will be emitted as they are available.
    *
    * @param persistenceId  the persistence ID to get events for
    * @param fromSequenceNr the sequence number to start getting events from
    * @param toSequenceNr   the maximum number to get events for (stream will not end until this has been reached.)
    * @return
    */
  def liveEventsByAuthorAndPeristenceId(
                                         author: String, persistenceId: String, fromSequenceNr: Long,
                                         toSequenceNr: Long
                                       ): Source[EventEnvelope, NotUsed] = {

    eventsByPersistenceIdSource(persistenceId, fromSequenceNr, toSequenceNr, true, author)
  }


  /**
    * Get all the instances who have their own version of the given persistenceId.
    * @param persistenceId the persistence ID to get the authors for
    * @return
    */
  def getAuthorsForPersistenceId(persistenceId: String): Future[Try[List[String]]] = {
    scuttlebuttDriver.getAuthorsForPersistenceId(persistenceId)
  }

  /**
    * Gets the identity (public key) of the scuttlebutt instance we're connected to
    *
    * @return
    */
  def getMyIdentity(): Future[Try[String]] = {
    scuttlebuttDriver.getMyIdentity()
  }

  /**
    *
    * @param authorId the ID of the author (instance) for the events
    * @param live whether this stream should remain open and emit any new persistence IDs for the given author (defaults to false)
    * @return
    */
  def getPersistenceIdsForAuthor(authorId: String, live: Boolean = false): Source[String, NotUsed] = {
    val pager = (start: Long, end: Long) => scuttlebuttDriver.getPersistenceIdsForAuthor(authorId, start, end, false)

    val pageStream = new PageStream[String](pager, scuttlebuttDriver, config, PageStream.defaultNextPage)

    if (live) {
      pageStream.getLiveStream()
    } else {
      pageStream.getStream()
    }
  }

  def getAllEventsForAuthor(authorId: String, live: Boolean = false): Source[EventEnvelope, NotUsed] = {
    getAllEventsForAuthorAfterSequenceNr(authorId, 0, live).map(_._1)
  }

  def getAllEventsForAuthorAfterSequenceNr(authorId: String, startingAt: Long, live: Boolean = false): Source[(EventEnvelope, Long), NotUsed] = {
    val getNextPageStart = (start: Long, result: Seq[(EventEnvelope, Long)]) => {
      if (result.length == 0) {
        start
      } else {
        result.last._2 + 1
      }

    }

    val pager = (start: Long, end: Long) => scuttlebuttDriver.getEventsForAuthor(authorId, start, end).map(
      _.map(_.map(toEnvelopeWithStartSequence(_)))
    )

    val pageStream = new PageStream[(EventEnvelope, Long)](pager, scuttlebuttDriver, config, getNextPageStart, startingAt)

    if (live) {
      pageStream.getLiveStream()
    } else {
      pageStream.getStream()
    }

  }

  /**
    * All the other authors currently in the system.
    * @return
    */
  def allOtherAuthors(): Future[Try[List[String]]] = {
    scuttlebuttDriver.getAllAuthors()
  }

  /**
    * A live stream of all the other visible authors in the scuttlebutt network.
    * The stream first emits currently known authors, and then emits new authors
    * if they become visible (e.g. if our instance followed them.)
    *
    * @return the author stream
    */
  def allAuthorsLive: Source[String, NotUsed] = {
    scuttlebuttDriver.getLiveAuthorStream()
  }


  /**
    * All the persistence IDs the current author (instance) has created. Stream remains open and emits new values
    * as new entities are created.
    *
    * @return
    */
  override def persistenceIds(): Source[String, NotUsed] = {
    getPersistenceIdsForAuthor(null, true)
  }

  /**
    * All the persistence IDs the current author (instance) has created. Stream remains open and emits new values
    * as new entities are created. The stream does not remain open.
    *
    * @return
    */
  override def currentPersistenceIds(): Source[String, NotUsed] = {

    getPersistenceIdsForAuthor(null, false)
  }

  override def eventsByTag(
                            tag: String, offset: Offset = Sequence(0L)): Source[EventEnvelope, NotUsed] = ???

  private def toEnvelopeWithStartSequence(rpcMessage: RPCResponse): (EventEnvelope, Long) = {

    val content: ObjectNode = rpcMessage.asJSON(objectMapper, classOf[ObjectNode])

    val sequence = content.get("scuttlebuttSequence").asLong()
    val eventEnvelope = toEnvelope(content)

    (eventEnvelope, sequence)
  }

  private def toEnvelope(rpcMessage: RPCResponse): EventEnvelope = {
    val content: ObjectNode = rpcMessage.asJSON(objectMapper, classOf[ObjectNode])

    toEnvelope(content)
  }

  private def toEnvelope(content: ObjectNode): EventEnvelope = {
    val payload: JsonNode = content.findPath("payload")
    val persistentRepr: PersistentRepr = objectMapper.treeToValue(content, classOf[PersistedMessage])

    val bytes = objectMapper.writeValueAsBytes(payload)
    val deserializePayload = scuttlebuttPersistenceSerializer.deserialize(bytes, persistentRepr.manifest).get

    new EventEnvelope(
      Sequence(persistentRepr.sequenceNr),
      persistentRepr.persistenceId,
      persistentRepr.sequenceNr,
      deserializePayload
    )

  }

  private def eventsByPersistenceIdSource(
                                           persistenceId: String,
                                           fromSequenceNr: Long,
                                           toSequenceNr: Long,
                                           live: Boolean,
                                           author: String = null): Source[EventEnvelope, NotUsed] = {

    /**
      * If the first event we see is an `AllowAccess` event with a sequence number greater than the one we queried for,
      * it indicates that we have just been given access to an entity with events that were previously not visible to us,
      * so we restart the stream to get the previous events. Otherwise, we start the stream as normal
      */
    val source = scuttlebuttDriver.eventsByPersistenceId(author, persistenceId, fromSequenceNr, toSequenceNr, live)
        .map(toEnvelope)

      source
      .take(1)
      .flatMapConcat({
        case (envelope: EventEnvelope)
          if envelope.sequenceNr > fromSequenceNr && envelope.event.isInstanceOf[AllowAccess] =>
            eventsByPersistenceIdSource(persistenceId, fromSequenceNr, toSequenceNr, live, author)
          case (envelope: EventEnvelope) => source
        }
      )

  }

}
