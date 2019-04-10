package org.openlaw.scuttlebutt.persistence

import akka.actor.ExtendedActorSystem
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.{ReadJournalProvider, javadsl}
import com.typesafe.config.Config
import org.openlaw.scuttlebutt.persistence.driver.{MultiplexerLoader, ScuttlebuttDriver}
import org.openlaw.scuttlebutt.persistence.serialization.ScuttlebuttPersistenceSerializationConfig

class ScuttlebuttReadJournalProvider(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {

  override def scaladslReadJournal(): ReadJournal = {

    val objectMapper = new ScuttlebuttPersistenceSerializationConfig().mapper

    val multiplexerLoader =  new MultiplexerLoader(objectMapper, config)
    val scuttlebuttDriver = new ScuttlebuttDriver(multiplexerLoader.loadMultiplexer, objectMapper)

    new ScuttlebuttReadJournal(system, config, scuttlebuttDriver, objectMapper)
  }

  override def javadslReadJournal(): javadsl.ReadJournal = {
    null

  }
}
