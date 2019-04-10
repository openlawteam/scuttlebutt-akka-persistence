package org.openlaw.scuttlebutt.persistence.query

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

class QueryBuilder(objectMapper: ObjectMapper) {



  def makeReplayQuery(
                       persistenceId: String,
                       fromSequenceNr: Long,
                       max: Long,
                       reverse: Boolean): ObjectNode = {

    makeReplayQuery(persistenceId, fromSequenceNr, None, max, reverse)
  }

  def makeReplayQuery(
                       persistenceId: String,
                       fromSequenceNr: Long,
                       toSequenceNr: Long,
                       max: Long,
                       reverse: Boolean): ObjectNode = {

    makeReplayQuery(persistenceId, fromSequenceNr, Some(toSequenceNr), max, reverse)
  }

  private def makeReplayQuery(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Option[Long], max: Long, reverse: Boolean = false): ObjectNode = {
    val rangeFilter = Map("$gte" -> fromSequenceNr) ++ toSequenceNr.fold(Map[String, Long]())(to => Map("$lte" -> to))

    // TODO: filter to just own public key as author. Also, make a query builder class and class representation of a query
    val query = Map(
      "query" ->
        List(
          Map("$filter" ->
            Map("value" ->
              Map("content" -> Map(
                ("type" -> persistenceId),
                ("sequenceNr" -> rangeFilter)
              )
              ))
          ),
          Map("$map" -> Map(
            "value" -> List("value"),
            "sequenceNr" -> List("value", "content", "sequenceNr")
          )),
          Map("$sort" -> List("sequenceNr"))
        ),
      "limit" -> max,
      "reverse" -> reverse)

    objectMapper.valueToTree(query)
  }

}
