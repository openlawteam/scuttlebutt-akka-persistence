package org.openlaw.scuttlebutt.persistence

import akka.Done
import akka.actor.ExtendedActorSystem
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.Config
import org.apache.tuweni.scuttlebutt.rpc.RPCResponse
import org.openlaw.scuttlebutt.persistence.driver.{ReconnectingScuttlebuttConnection, ScuttlebuttDriver}
import org.openlaw.scuttlebutt.persistence.serialization.{PersistedMessage, ScuttlebuttPersistenceSerializer}

import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, _}
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

class ScuttlebuttAsyncWriteJournal(config: Config) extends AsyncWriteJournal {

  implicit val executionContext = context.system.dispatcher
  implicit val materializer = ActorMaterializer()

  val serializationConfig = new ScuttlebuttPersistenceSerializer(context.system)

  val objectMapper: ObjectMapper = serializationConfig.getObjectMapper()
  val scuttlebuttPersistenceSerializer = new ScuttlebuttPersistenceSerializer(context.system)

  val driver = ReconnectingScuttlebuttConnection(context.system, config, 1 minute)

  val scuttlebuttDriver: ScuttlebuttDriver = new ScuttlebuttDriver(context.system, driver, objectMapper, scuttlebuttPersistenceSerializer)

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {

    if (containsUnsupportedBulkWrite(messages)) {
      Future.failed(new UnsupportedOperationException("Scuttlebutt backend does not currently support atomic bulk writes."))
    } else {
      val individualMessages: Seq[PersistentRepr] = messages.map(_.payload).flatten.sortBy(_.sequenceNr)
      val emptySeq: immutable.Seq[Try[Unit]] = immutable.Seq()

      // We perform each write sequentially, and if any of the previous writes fail we do not attempt any
      // subsequent writes
      individualMessages.foldRight[Future[immutable.Seq[Try[Unit]]]](Future.successful(emptySeq)) {
        (req: PersistentRepr, result: Future[immutable.Seq[Try[Unit]]]) => {
          result.flatMap(statuses => {
            statuses.find(result => result.isFailure) match {
              case Some(x) => Future.successful(statuses :+ Failure(new Exception("Previous write failed")))
              case None => {
                // publish and add to the list of results we're building
                scuttlebuttDriver.publishPersistentRep(req).map(result => statuses :+ result)
              }
            }
          })
        }
      }
    }
  }

  def containsUnsupportedBulkWrite(messages: immutable.Seq[AtomicWrite]): Boolean = {
    return messages.find(msg => msg.payload.length > 1).isDefined
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long) =
    throw new UnsupportedOperationException("Deletions are not yet supported.")

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
    recoveryCallback: PersistentRepr => Unit
  ): Future[Unit] = {

    val events = scuttlebuttDriver.myEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr, false)

    events
      .map(deserializeIntoPersistentRepl)
      .runForeach({
        case Success(repr) => recoveryCallback(repr)
        // Indicate failure to the returned Future (satisfying the method contract to fail the future if any item fails),
        // and stop the stream
        case Failure(ex) => throw ex
      }).map(_ => {})

  }

  private def deserializeIntoPersistentRepl(rpcMessage: RPCResponse): Try[PersistentRepr] = {
    val content:ObjectNode = rpcMessage.asJSON(objectMapper, classOf[ObjectNode])
    val payload: JsonNode = content.findPath("payload")
    val payloadBytes = objectMapper.writeValueAsBytes(payload)

    val persistentRepr : PersistentRepr = objectMapper.treeToValue(content, classOf[PersistedMessage])
    val deserializePayload = scuttlebuttPersistenceSerializer.deserialize(payloadBytes,  persistentRepr.manifest)

    deserializePayload map {
      value => persistentRepr.withPayload(value)
    }

  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    scuttlebuttDriver.getHighestSequenceNr(persistenceId)
  }

}