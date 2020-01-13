package org.openlaw.scuttlebutt.persistence

import akka.actor.{ExtendedActorSystem, Props}
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.{ReadJournalProvider, javadsl}
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.Config
import org.openlaw.scuttlebutt.persistence.driver.{ReconnectingScuttlebuttConnection, ScuttlebuttDriver, ScuttlebuttDriverActor}
import org.openlaw.scuttlebutt.persistence.serialization.ScuttlebuttPersistenceSerializer

import scala.concurrent.duration._

class ScuttlebuttReadJournalProvider(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {

  override def scaladslReadJournal(): ReadJournal = {
    scalaJournal()
  }

  override def javadslReadJournal(): javadsl.ReadJournal = {
    new ScuttlebuttJavaReadJournalProvider(scalaJournal())
  }

  private def scalaJournal(): ScuttlebuttReadJournal = {

    val serializationConfig = new ScuttlebuttPersistenceSerializer(system)

    val objectMapper: ObjectMapper = serializationConfig.getObjectMapper()
    val scuttlebuttPersistenceSerializer = new ScuttlebuttPersistenceSerializer(system)

    val driver = ReconnectingScuttlebuttConnection(system, config, 5 seconds)

    val scuttlebuttDriver: ScuttlebuttDriver = new ScuttlebuttDriver(system, driver, objectMapper, scuttlebuttPersistenceSerializer)
    new ScuttlebuttReadJournal(system, config, scuttlebuttDriver, objectMapper, scuttlebuttPersistenceSerializer)

  }

}
