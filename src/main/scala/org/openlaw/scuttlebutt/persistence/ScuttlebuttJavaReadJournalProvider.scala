package org.openlaw.scuttlebutt.persistence
import akka.NotUsed
import akka.persistence.query.{EventEnvelope, Offset}
import akka.stream.javadsl.Source

class ScuttlebuttJavaReadJournalProvider(scalaScuttlebuttReader: ScuttlebuttReadJournal) extends akka.persistence.query.javadsl.ReadJournal
  with akka.persistence.query.javadsl.EventsByTagQuery
  with akka.persistence.query.javadsl.EventsByPersistenceIdQuery
  with akka.persistence.query.javadsl.PersistenceIdsQuery
  with akka.persistence.query.javadsl.CurrentPersistenceIdsQuery {

  override def eventsByTag(tag: String, offset: Offset): Source[EventEnvelope, NotUsed] = {
    scalaScuttlebuttReader.eventsByTag(tag, offset).asJava
  }

  override def eventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, NotUsed] = {
    scalaScuttlebuttReader.eventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava
  }

  override def persistenceIds(): Source[String, NotUsed] = {
    scalaScuttlebuttReader.persistenceIds().asJava
  }

  override def currentPersistenceIds(): Source[String, NotUsed] = {
    scalaScuttlebuttReader.currentPersistenceIds().asJava
  }
}
