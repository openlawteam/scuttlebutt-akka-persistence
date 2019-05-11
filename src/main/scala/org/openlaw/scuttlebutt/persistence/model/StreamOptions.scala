package org.openlaw.scuttlebutt.persistence.model

/**
  *
  * @param start the start result (for pagination.)
  * @param end the end result (for pagination.)
  */
case class StreamOptions(start: Long, end: Long, reverse: Boolean)