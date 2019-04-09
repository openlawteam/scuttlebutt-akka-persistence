package org.openlaw.scuttlebutt.persistence

import akka.NotUsed
import akka.persistence.query.EventEnvelope
import akka.persistence.query.scaladsl.ReadJournal
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import net.consensys.cava.scuttlebutt.rpc.mux.Multiplexer
import org.openlaw.scuttlebutt.persistence.driver.ScuttlebuttDriver

class ScuttlebuttReadJournal(config: Config, scuttlebuttDriver: ScuttlebuttDriver) extends ReadJournal {



  def eventsByPersistenceId(
                                      persistenceId: String, fromSequenceNr: Long,
                                      toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {

    null

  }




}
