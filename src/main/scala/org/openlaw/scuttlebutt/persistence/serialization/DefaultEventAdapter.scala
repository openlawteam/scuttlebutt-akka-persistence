package org.openlaw.scuttlebutt.persistence.serialization

import akka.actor.ExtendedActorSystem
import akka.persistence.journal.{EventAdapter, EventSeq}
import com.fasterxml.jackson.databind.ObjectMapper

/**
  *
  */
class DefaultEventAdapter(actorSystem: ExtendedActorSystem) extends EventAdapter {

  val objectMapper: ObjectMapper = new ScuttlebuttPersistenceSerializationConfig().getObjectMapper()

  override def fromJournal(event: Any, manifest: String): EventSeq = {

    val className = manifest
    val clazz: Class[_] = actorSystem.dynamicAccess.getClassFor(className)
      .getOrElse(Class.forName(className, true, Thread.currentThread.getContextClassLoader))

    val deserializedPayload = objectMapper.readValue(event.toString(), clazz)

    EventSeq.single(deserializedPayload)
  }

  override def manifest(event: Any): String = ""

  override def toJournal(event: Any): Any = {
    event
  }
}
