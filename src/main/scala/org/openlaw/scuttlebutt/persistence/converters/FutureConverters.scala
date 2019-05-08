package org.openlaw.scuttlebutt.persistence.converters

import org.apache.tuweni.concurrent.AsyncResult

import scala.concurrent.{Future, Promise}

object FutureConverters {

  /**
    * Converts a cava AsyncResult to a scala Future
    *
    * @param result the async result to convert
    * @tparam T the result of the future when successfully completed
    *
    * @return a scala future which will be completed with the same result as a the async result
    */
  implicit def asyncResultToFuture[T](result: AsyncResult[T]): Future[T] = {

    val promise = Promise[T]

    result.whenComplete((result, exception) => {
      if (exception != null) {
        promise.failure(exception)
      } else {
        promise.success(result)
      }
    })

    promise.future
  }
}
