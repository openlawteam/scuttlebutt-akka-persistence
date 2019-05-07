package org.openlaw.scuttlebutt.persistence.reader

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.openlaw.scuttlebutt.persistence.driver.ScuttlebuttDriver

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext.Implicits.global


case class PageStream[T](pager: (Long, Long) => Future[Try[Seq[T]]],
                         scuttlebuttDriver: ScuttlebuttDriver,
                         config: Config) {


  def getStream(): Source[T, NotUsed] = {

    val step = config.getInt("max-buffer-size")

    val eventSource = Source.unfoldAsync[Long, Seq[T]](0) {
      case start => {
        val end = start + step

        pager(start, end).map {
          case Success(result) if result.isEmpty => {
            None
          }
          case Success(result) => {
            Some((start + result.length) -> result)
          }
          case Failure(exception) => throw exception
        }

      }

    }

    eventSource.flatMapConcat(events => Source.fromIterator(() => events.iterator))
  }

  def getLiveStream(): Source[T, NotUsed] = {

    val step = config.getInt("max-buffer-size")

    val eventSource = Source.unfoldAsync[Long, Seq[T]](0) {
      case start => {
        val end = start + step

        pollUntilAvailable(pager, start, end).map {

          case Success(result) if result.isEmpty => {
            Some((start + result.length) -> result)
          }
          case Success(result) => {
            Some((start + result.length) -> result)
          }
          case Failure(exception) => throw exception
        }
      }
    }

    eventSource.flatMapConcat(events => Source.fromIterator(() => events.iterator))

  }

  def pollUntilAvailable(pager: (Long, Long) => Future[Try[Seq[T]]], start: Long, end: Long): Future[Try[Seq[T]]] = {
    pager(start, end).flatMap {
      case Success(events) if events.isEmpty => {

        Thread.sleep(config.getDuration("refresh-interval").toMillis)
        pager(start, end)
      }
      case result: Success[Seq[T]] => Future.successful(result)
    }
  }
}
