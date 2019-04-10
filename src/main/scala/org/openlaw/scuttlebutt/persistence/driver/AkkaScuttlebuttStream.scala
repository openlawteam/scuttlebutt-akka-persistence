package org.openlaw.scuttlebutt.persistence.driver

import akka.persistence.PersistentRepr
import akka.persistence.query.{EventEnvelope, Sequence}

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import com.fasterxml.jackson.databind.ObjectMapper
import net.consensys.cava.scuttlebutt.rpc.RPCMessage
import net.consensys.cava.scuttlebutt.rpc.mux.ScuttlebuttStreamHandler
import org.openlaw.scuttlebutt.persistence.query.QueryBuilder

import scala.concurrent.Future

class AkkaScuttlebuttStream(persistenceId: String,
                             fromSequenceNr: Long,
                             toSequenceNr: Long,
                             driver: ScuttlebuttDriver,
                             objectMapper: ObjectMapper) extends GraphStage[SourceShape[EventEnvelope]] {

  val out: Outlet[EventEnvelope] = Outlet("ScuttlebuttQuerySource")

  val queryBuilder = new QueryBuilder(objectMapper)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {

    new GraphStageLogic(shape) {

      val query = queryBuilder.makeReplayQuery(persistenceId, fromSequenceNr, toSequenceNr, Integer.MAX_VALUE, false )





    }

  }

  override def shape: SourceShape[EventEnvelope] = SourceShape(out)
}
