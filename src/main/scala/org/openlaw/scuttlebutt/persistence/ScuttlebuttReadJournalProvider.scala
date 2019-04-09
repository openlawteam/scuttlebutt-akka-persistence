package org.openlaw.scuttlebutt.persistence

import akka.actor.ExtendedActorSystem
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.{ReadJournalProvider, javadsl}
import com.typesafe.config.Config

class ScuttlebuttReadJournalProvider(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {

  override def scaladslReadJournal(): ReadJournal = ???

  override def javadslReadJournal(): javadsl.ReadJournal = ???
}
