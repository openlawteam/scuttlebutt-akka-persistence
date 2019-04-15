package org.openlaw.scuttlebutt.persistence

import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.Config
import net.consensys.cava.scuttlebutt.rpc.mux.{RPCHandler, ScuttlebuttStreamHandler}
import net.consensys.cava.scuttlebutt.rpc._
import org.openlaw.scuttlebutt.persistence.driver.{MultiplexerLoader, ScuttlebuttDriver}
import org.openlaw.scuttlebutt.persistence.query.QueryBuilder
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

  val queryBuilder = new QueryBuilder(objectMapper)

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

    val query = queryBuilder.makeReplayQuery(persistenceId, fromSequenceNr, toSequenceNr, max, false)

    scuttlebuttDriver.openQueryStream(query, (closer: Runnable) => {
      new PersistentReprStreamHandler(objectMapper, closer, recoveryCallback, finishedReplaysPromise)
    })

    finishedReplaysPromise.future
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    // There is a ssb-query bug whereby 'reverse' is ignored if a sort value is specified preventing us
    // doing this the smart way (with a query that has a limit of 1, sorts in descending and a limit of 1,
    // we work around by reading the full stream to the end for now as a workaround

    val result = Promise[Long]()
    val query = queryBuilder.makeReplayQuery(persistenceId, fromSequenceNr, Long.MaxValue, reverse = false)

    var currentMax = 0l

    scuttlebuttDriver.openQueryStream(query, (closer: Runnable) => {
      new ScuttlebuttStreamHandler() {
        override def onMessage(rpcMessage: RPCResponse): Unit = {
          val node: ObjectNode = rpcMessage.asJSON(objectMapper, classOf[ObjectNode])
          val content: JsonNode = node.findPath("content")

          val responseBody: PersistentRepr = objectMapper.treeToValue(content, classOf[PersistedMessage])

          currentMax = responseBody.sequenceNr
        }

        override def onStreamEnd(): Unit = {
            result.success(currentMax)
        }

        override def onStreamError(e: Exception): Unit = {
          result.failure(e)
        }
      }
    })

    return result.future
  }

}