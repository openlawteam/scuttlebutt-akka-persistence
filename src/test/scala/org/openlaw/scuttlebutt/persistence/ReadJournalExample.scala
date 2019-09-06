package org.openlaw.scuttlebutt.persistence

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink

import scala.concurrent.ExecutionContext

object ReadJournalExample {

  implicit val ec: ExecutionContext = ExecutionContext.global

  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("HelloSystem")
    implicit val materializer = ActorMaterializer()

    val readJournal = PersistenceQuery(system).readJournalFor[ScuttlebuttReadJournal](
      "org.openlaw.scuttlebutt.journal.persistence"
    )

    val source = readJournal.currentEventsByPersistenceId(
      "sample-id-6", 0, 101
    )
    //source.runWith(Sink.foreach(println))

    readJournal.getMyIdentity().foreach(println(_))

    val allPersistenceIdsSource = readJournal.currentPersistenceIds()

    allPersistenceIdsSource.runWith(Sink.foreach(println))

    readJournal.getAllEventsForAuthor(null, live=true).runWith(Sink.foreach(println))

    readJournal.allOtherAuthors().foreach(println)

  }


}
