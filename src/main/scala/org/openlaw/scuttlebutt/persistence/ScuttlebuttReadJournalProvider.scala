package org.openlaw.scuttlebutt.persistence

import akka.actor.ExtendedActorSystem
import akka.persistence.Persistence
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.{ReadJournalProvider, javadsl}
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.Config
import org.openlaw.scuttlebutt.persistence.driver.{MultiplexerLoader, ScuttlebuttDriver}
import org.openlaw.scuttlebutt.persistence.serialization.ScuttlebuttPersistenceSerializer

class ScuttlebuttReadJournalProvider(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {

  override def scaladslReadJournal(): ReadJournal = {
    scalaJournal()
  }

  override def javadslReadJournal(): javadsl.ReadJournal = {
    new ScuttlebuttJavaReadJournalProvider(scalaJournal())
  }

  private def scalaJournal(): ScuttlebuttReadJournal = {

    val serializer =  new ScuttlebuttPersistenceSerializer(system)
    val objectMapper = serializer.getObjectMapper()

    val multiplexerLoader =  new MultiplexerLoader(objectMapper, config)

    multiplexerLoader.loadMultiplexer match {
      case Left(error) => throw new Exception(error)
      case Right(rpcHandler) => {
        val scuttlebuttDriver = new ScuttlebuttDriver(rpcHandler, objectMapper, serializer)
        new ScuttlebuttReadJournal(system, config, scuttlebuttDriver, objectMapper, serializer)
      }
    }

  }

}
