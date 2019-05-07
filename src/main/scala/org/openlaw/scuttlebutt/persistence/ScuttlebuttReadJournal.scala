package org.openlaw.scuttlebutt.persistence

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.persistence.{Persistence, PersistentRepr}
import akka.persistence.query.{EventEnvelope, Offset, Sequence}
import akka.persistence.query.scaladsl.ReadJournal
import akka.stream.scaladsl.Source
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.config.Config
import net.consensys.cava.scuttlebutt.rpc.RPCResponse
import org.openlaw.scuttlebutt.persistence.driver.ScuttlebuttDriver
import org.openlaw.scuttlebutt.persistence.reader.{PageStream, ScuttlebuttStreamRangeFiller}
import org.openlaw.scuttlebutt.persistence.serialization.PersistedMessage

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}


class ScuttlebuttReadJournal(
                              system: ExtendedActorSystem,
                              config: Config,
                              scuttlebuttDriver: ScuttlebuttDriver,
                              objectMapper: ObjectMapper) extends ReadJournal
  with akka.persistence.query.scaladsl.EventsByTagQuery
  with akka.persistence.query.scaladsl.EventsByPersistenceIdQuery
  with akka.persistence.query.scaladsl.CurrentEventsByPersistenceIdQuery
  with akka.persistence.query.scaladsl.PersistenceIdsQuery
  with akka.persistence.query.scaladsl.CurrentPersistenceIdsQuery {


  val eventAdapters = Persistence(system).adaptersFor("scuttlebutt-journal")

  val rangeFiller = new ScuttlebuttStreamRangeFiller(scuttlebuttDriver, objectMapper)

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    eventsByPersistenceIdSource(persistenceId, fromSequenceNr, toSequenceNr, false)
  }

  override def eventsByPersistenceId(
                                      persistenceId: String, fromSequenceNr: Long,
                                      toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    eventsByPersistenceIdSource(persistenceId, fromSequenceNr, toSequenceNr, true)
  }

  def eventsByAuthorAndPeristenceId(
                                     author: String, persistenceId: String, fromSequenceNr: Long,
                                     toSequenceNr: Long
                                   ): Source[EventEnvelope, NotUsed] = {

    eventsByPersistenceIdSource(persistenceId, fromSequenceNr, toSequenceNr, true, author)
  }

  def getAuthorsForPersistenceId(persistenceId: String): Future[Try[List[String]]] = {
    scuttlebuttDriver.getAuthorsForPersistenceId(persistenceId)
  }

  def getPersistenceIdsForAuthor(authorId: String, live: Boolean = false) : Source[String, NotUsed] = {
    val pager = (start: Long, end: Long) => scuttlebuttDriver.getPersistenceIdsForAuthor(authorId, start, end, true)

    val pageStream = new PageStream[String](pager, scuttlebuttDriver, config)

    if (live) {
      pageStream.getLiveStream()
    } else {
      pageStream.getStream()
    }
  }

  def allAuthors(): Future[Try[List[String]]] = {
    scuttlebuttDriver.getAllAuthors()
  }


  override def persistenceIds(): Source[String, NotUsed] = {
    getPersistenceIdsForAuthor(null, true)
  }

  override def currentPersistenceIds(): Source[String, NotUsed] = {

    getPersistenceIdsForAuthor(null, false)
  }

  override def eventsByTag(
                            tag: String, offset: Offset = Sequence(0L)): Source[EventEnvelope, NotUsed] = ???

  private def pollUntilAvailable(persistenceId: String, start: Long, max: Long, end: Long, author: String = null): Future[Seq[EventEnvelope]] = {
    // Scuttlebutt does not implement back pressure so (like many other persistence plugins) we have to poll until
    // more is available

    rangeFiller.getEventMessages(persistenceId, start, max, end, author).map(events => events.map(toEnvelope)).flatMap {
      case events if events.isEmpty => {

        Thread.sleep(config.getDuration("refresh-interval").toMillis)
        pollUntilAvailable(persistenceId, start, max, end, author)
      }
      case events => Future.successful(events)
    }
  }

  private def toEnvelope(rpcMessage: RPCResponse): EventEnvelope = {
    val content: ObjectNode = rpcMessage.asJSON(objectMapper, classOf[ObjectNode])

    val payload: JsonNode = content.findPath("payload")

    val persistentRepr: PersistentRepr = objectMapper.treeToValue(content, classOf[PersistedMessage])

    val eventAdapter = eventAdapters.get(payload.getClass)

    val deserializedPayload = eventAdapter.fromJournal(payload, persistentRepr.manifest).events.head

    new EventEnvelope(
      Sequence(persistentRepr.sequenceNr),
      persistentRepr.persistenceId,
      persistentRepr.sequenceNr,
      deserializedPayload
    )
  }

  private def eventsByPersistenceIdSource(
                                           persistenceId: String,
                                           fromSequenceNr: Long,
                                           toSequenceNr: Long,
                                           live: Boolean,
                                           author: String = null): Source[EventEnvelope, NotUsed] = {
    val step = config.getInt("max-buffer-size")

    val eventSource = Source.unfoldAsync[Long, Seq[EventEnvelope]](0) {

      case start if start >= toSequenceNr => Future.successful(None)
      case start => {
        val end = Math.min(start + step, toSequenceNr)

        if (live) {
          pollUntilAvailable(persistenceId, start, step, end, author).map(
            results => Some((start + results.length) -> results)
          )
        } else {
          rangeFiller.getEventMessages(persistenceId, start, step, end, author)
            .map(rpcMessages => rpcMessages.map(toEnvelope)).map {
            case events if events.isEmpty => None
            case events => Some((start + events.length) -> events)
          }

        }
      }
    }

    eventSource.flatMapConcat(events => Source.fromIterator(() => events.iterator))
  }

}
