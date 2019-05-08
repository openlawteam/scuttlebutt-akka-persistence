package org.openlaw.scuttlebutt.persistence

import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.Config
import org.apache.tuweni.scuttlebutt.rpc.mux.{RPCHandler, ScuttlebuttStreamHandler}
import org.apache.tuweni.scuttlebutt.rpc._
import org.openlaw.scuttlebutt.persistence.driver.{MultiplexerLoader, ScuttlebuttDriver}

import org.openlaw.scuttlebutt.persistence.serialization.{PersistedMessage, ScuttlebuttPersistenceSerializationConfig}

import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, _}
import scala.util.{Failure, Try}

class ScuttlebuttAsyncWriteJournal(config: Config) extends AsyncWriteJournal {

  val objectMapper: ObjectMapper = new ScuttlebuttPersistenceSerializationConfig().getObjectMapper()
  val loader: MultiplexerLoader = new MultiplexerLoader(objectMapper, config)

  val rpcHandler: RPCHandler = loader.loadMultiplexer
  val scuttlebuttDriver: ScuttlebuttDriver = new ScuttlebuttDriver(rpcHandler, objectMapper)

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
  ) = {
    val finishedReplaysPromise = Promise[Unit]();

    scuttlebuttDriver.myEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr, (closer: Runnable) => {
      new PersistentReprStreamHandler(objectMapper, closer, recoveryCallback, finishedReplaysPromise)
    })

    finishedReplaysPromise.future
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    scuttlebuttDriver.getHighestSequenceNr(persistenceId)
  }

}