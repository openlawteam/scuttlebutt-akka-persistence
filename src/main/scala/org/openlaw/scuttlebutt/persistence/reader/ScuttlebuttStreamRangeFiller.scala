package org.openlaw.scuttlebutt.persistence.reader

import com.fasterxml.jackson.databind.ObjectMapper
import net.consensys.cava.scuttlebutt.rpc.RPCMessage
import net.consensys.cava.scuttlebutt.rpc.mux.ScuttlebuttStreamHandler
import org.openlaw.scuttlebutt.persistence.driver.ScuttlebuttDriver
import org.openlaw.scuttlebutt.persistence.query.QueryBuilder

import scala.concurrent.{Future, Promise}

class ScuttlebuttStreamRangeFiller(
                                    driver: ScuttlebuttDriver,
                                      objectMapper: ObjectMapper,
                                  ) {

  val queryBuilder = new QueryBuilder(objectMapper)

  def getEventMessages(persistenceId: String,
                       fromSequenceNr: Long,
                       max: Long,
                       toSequenceNr: Long): Future[Seq[RPCMessage]] = {

    var promise: Promise[Seq[RPCMessage]] = Promise()

    var query = queryBuilder.makeReplayQuery(persistenceId, fromSequenceNr, toSequenceNr, max, false)

    driver.openQueryStream(query, (stopper) => {
      new ScuttlebuttStreamHandler {
        var results: Seq[RPCMessage] = Seq()

        override def onMessage(message: RPCMessage): Unit = {

          if (!message.isErrorMessage) {
            results = results :+ message
          } else {
            val errorMessage = message.getErrorBody(objectMapper)
              .transform(errorBody => errorBody.getMessage)
              .or(message.asString())

            promise.failure(new Exception(errorMessage))
          }
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
