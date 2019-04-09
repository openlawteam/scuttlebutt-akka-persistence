package org.openlaw.scuttlebutt.persistence

import akka.persistence.journal.{EventAdapter, EventSeq}
import com.fasterxml.jackson.databind.ObjectMapper
import org.openlaw.scuttlebutt.persistence.serialization.ScuttlebuttPersistenceSerializationConfig

/**
  *
  */
class DefaultEventAdapter extends EventAdapter {

  val objectMapper: ObjectMapper = new ScuttlebuttPersistenceSerializationConfig().getObjectMapper()

  override def fromJournal(event: Any, manifest: String): EventSeq = {

    val className = manifest
    val clazz: Class[_] = Class.forName(className)

    val deserializedPayload = objectMapper.readValue(event.toString(), clazz)

    EventSeq.single(deserializedPayload)
  }

  override def manifest(event: Any): String = ""

  override def toJournal(event: Any): Any = {
    event
  }
}
