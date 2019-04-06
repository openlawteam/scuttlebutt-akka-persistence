package org.openlaw.scuttlebutt.persistence

import java.io.{BufferedWriter, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets.UTF_8
import java.util
import java.util.function.Consumer

import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.{Serialization, SerializationExtension}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.Config
import io.vertx.core.Vertx
import net.consensys.cava.bytes.{Bytes, Bytes32}
import net.consensys.cava.concurrent.AsyncResult
import net.consensys.cava.crypto.sodium.Box.KeyPair
import net.consensys.cava.crypto.sodium.Signature
import net.consensys.cava.io.Base64
import net.consensys.cava.scuttlebutt.handshake.vertx.SecureScuttlebuttVertxClient
import net.consensys.cava.scuttlebutt.rpc.mux.exceptions.ConnectionClosedException
import net.consensys.cava.scuttlebutt.rpc.mux.{Multiplexer, RPCHandler, ScuttlebuttStreamHandler}
import net.consensys.cava.scuttlebutt.rpc.{RPCAsyncRequest, RPCFunction, RPCMessage, RPCStreamRequest}
import org.logl.Level
import org.logl.logl.SimpleLogger

import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, _}
import scala.util.{Failure, Success, Try}


class ScuttlebuttAsyncWriteJournal(config: Config) extends AsyncWriteJournal {

  val objectMapper: ObjectMapper = new ScuttlebuttPersistenceSerializationConfig().getObjectMapper()
  val rpcHandler: Multiplexer = getMultiplexer()

  def getMultiplexer(): Multiplexer = {

    val keyPair: Option[Signature.KeyPair] = KeyUtils.getLocalKeys()

    // TODO: make it possible to modify the host, etc, in the akka config

    if (!keyPair.isDefined) {
      // TODO: more specific exception type
      throw new Exception("Could not find local scuttlebutt keys.")
    }

    val localKeys = keyPair.get

    val networkKeyBase64 = "1KHLiKZvAvjbY1ziZEHMXawbCEIM6qwjCDm3VYRan/s="
    val networkKeyBytes32 = Bytes32.wrap(Base64.decode(networkKeyBase64))
    val host = "localhost"
    val port = 8008
    val vertx = Vertx.vertx()
    val loggerProvider = SimpleLogger.withLogLevel(Level.DEBUG).toPrintWriter(new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out, UTF_8))))
    val secureScuttlebuttVertxClient = new SecureScuttlebuttVertxClient(loggerProvider, vertx, localKeys, networkKeyBytes32)

    val onConnect: AsyncResult[RPCHandler] = secureScuttlebuttVertxClient.connectTo(port, host, localKeys.publicKey, (sender: Consumer[Bytes], terminationFn: Runnable) => {
      def makeHandler(sender: Consumer[Bytes], terminationFn: Runnable) = new RPCHandler(sender, terminationFn, objectMapper, loggerProvider)

      makeHandler(sender, terminationFn)
    })

    onConnect.get

  }

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
    val promise: Promise[Try[Unit]] = Promise[Try[Unit]]();

    val result: AsyncResult[RPCMessage] = rpcHandler.makeAsyncRequest(request)

    // Convert from AsyncResult from Cava to scala Future class
    result.whenComplete((result, exception) => {
      // The AsyncWriteJournal docs requires that the future is completed exceptionally only if there is a
      // connection error, otherwise the inner 'try' should be a 'Failure'
      if (exception != null && exception.isInstanceOf[ConnectionClosedException]) {
        // The connection broke
        promise.failure(exception)
      } else if (exception != null) {
        // An exception was thrown by our own process
        promise.success(Failure(exception))
      } else {

        // An error was returned as the RPC response

        if (result.lastMessageOrError()) {
          val exception = new Exception("RPC request failed") // todo: more descriptive error from contents
          promise.success(Failure(exception))
        } else {
          promise.success(Success())
        }
      }

    })

    return promise.future
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