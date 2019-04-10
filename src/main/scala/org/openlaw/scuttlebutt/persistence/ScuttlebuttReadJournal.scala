package org.openlaw.scuttlebutt.persistence

import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.persistence.{Persistence, PersistentRepr}
import akka.persistence.query.{EventEnvelope, Sequence}
import akka.persistence.query.scaladsl.ReadJournal
import akka.stream.scaladsl.{Flow, Source}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.config.Config
import net.consensys.cava.scuttlebutt.rpc.RPCMessage
import org.openlaw.scuttlebutt.persistence.driver.ScuttlebuttDriver
import org.openlaw.scuttlebutt.persistence.reader.ScuttlebuttStreamRangeFiller
import org.openlaw.scuttlebutt.persistence.serialization.PersistedMessage

import scala.collection.immutable.NumericRange
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


class ScuttlebuttReadJournal(
                              system: ExtendedActorSystem,
                              config: Config,
                              scuttlebuttDriver: ScuttlebuttDriver,
                              objectMapper: ObjectMapper) extends ReadJournal {


  val eventAdapters = Persistence(system).adaptersFor("scuttlebutt-journal")

  val rangeFiller = new ScuttlebuttStreamRangeFiller(scuttlebuttDriver, objectMapper)

  def eventsByPersistenceId(
                             persistenceId: String, fromSequenceNr: Long,
                             toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {

    val step = config.getInt("max-buffer-size")

    val eventSource = Source.unfoldAsync[Long, Seq[EventEnvelope]](0) {

      case start if start > toSequenceNr => Future.successful(None)
      case start => {
        val end = Math.min(start + step, toSequenceNr)

        pollUntilAvailable(persistenceId, start, step, end).map(
          results => Some((start + results.length) -> results)
        )
      }
    }

    eventSource.flatMapConcat(events => Source.fromIterator(() => events.iterator))
  }

  private def pollUntilAvailable(persistenceId: String, start: Long, max: Long, end: Long): Future[Seq[EventEnvelope]] = {
    // Scuttlebutt does not implement back pressure so (like many other persistence plugins) we have to poll until
    // more is available

    rangeFiller.getEventMessages(persistenceId, start, max, end).map(events => events.map(toEnvelope)).flatMap {
      case events if events.isEmpty => {

        Thread.sleep(config.getDuration("refresh-interval").toMillis)
        pollUntilAvailable(persistenceId, start, max, end)
      }
      case events => Future.successful(events)
    }
  }


  private def toEnvelope(rpcMessage: RPCMessage): EventEnvelope = {
    val node: ObjectNode = rpcMessage.asJSON(objectMapper, classOf[ObjectNode])

    val content: JsonNode = node.findPath("content")
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


}
