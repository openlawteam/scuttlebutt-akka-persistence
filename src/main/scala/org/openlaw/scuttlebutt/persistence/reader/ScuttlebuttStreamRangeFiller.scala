package org.openlaw.scuttlebutt.persistence.reader

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.tuweni.scuttlebutt.rpc.RPCResponse
import org.apache.tuweni.scuttlebutt.rpc.mux.ScuttlebuttStreamHandler
import org.openlaw.scuttlebutt.persistence.driver.ScuttlebuttDriver

import scala.concurrent.{Future, Promise}

/**
  * Provides helper methods to consume a scuttlebutt stream into an array
  *
  * @param driver the scuttlebutt driver for making requests
  * @param objectMapper the object mapper for deserialization
  */
class ScuttlebuttStreamRangeFiller(
                                    driver: ScuttlebuttDriver,
                                      objectMapper: ObjectMapper,
                                  ) {

  /**
    *
    * @param persistenceId the persistence ID for the messages
    * @param fromSequenceNr the start sequence number (inclusive.)
    * @param max the maximum number of results to fetch
    * @param toSequenceNr the end sequence number (inclusive.)
    * @param authorId the author of the messages or null if ourselves
    * @return a future which will be populated with only successful RPC messages, or completed exceptionally if
    *         the request failed for any reason
    */
  def getEventMessages(persistenceId: String,
                       fromSequenceNr: Long,
                       max: Long,
                       toSequenceNr: Long,
                       author: String = null): Future[Seq[RPCResponse]] = {

    var promise: Promise[Seq[RPCResponse]] = Promise()

    driver.eventsByPersistenceId(author, persistenceId, fromSequenceNr, toSequenceNr, (stopper) => {
      new ScuttlebuttStreamHandler {
        var results: Seq[RPCResponse] = Seq()

        override def onMessage(message: RPCResponse): Unit = {
          results = results :+ message
        }

        override def onStreamEnd(): Unit = {
          promise.success(results)
        }

        override def onStreamError(ex: Exception): Unit = {
          promise.failure(ex)
        }
      }
    })

    return promise.future
  }


}
