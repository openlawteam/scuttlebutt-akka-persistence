package org.openlaw.scuttlebutt.persistence

import akka.NotUsed
import akka.persistence.query.EventEnvelope
import akka.persistence.query.scaladsl.ReadJournal
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import net.consensys.cava.scuttlebutt.rpc.mux.Multiplexer

class ScuttlebuttReadJournal(config: Config, driver: Multiplexer) extends ReadJournal {



  def eventsByPersistenceId(
                                      persistenceId: String, fromSequenceNr: Long,
                                      toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    null




  }




}
