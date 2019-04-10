package org.openlaw.scuttlebutt.persistence

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object ReadJournalExample {

  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("HelloSystem")
    implicit val materializer = ActorMaterializer()

    val readJournal = PersistenceQuery(system).readJournalFor[ScuttlebuttReadJournal](
      "org.openlaw.scuttlebutt.journal.persistence"
    )

    val source = readJournal.eventsByPersistenceId(
      "sample-id-4", 0, 10000
    )

    source.runWith(Sink.foreach(println))

  }


}
