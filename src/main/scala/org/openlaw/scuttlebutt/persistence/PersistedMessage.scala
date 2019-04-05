package org.openlaw.scuttlebutt.persistence

import akka.actor.ActorRef
import akka.persistence.PersistentRepr
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import sun.reflect.generics.reflectiveObjects.NotImplementedException

@JsonIgnoreProperties(ignoreUnknown = true)
case class PersistedMessage (
                              payload: Any,
                              manifest: String,
                              persistenceId: String,
                              sequenceNr: Long,
                              writerUuid: String,
                              deleted: Boolean,
                              sender: ActorRef) extends PersistentRepr {


  override def withPayload(payload: Any): PersistentRepr = {
    copy(payload = payload)
  }

  override def withManifest(manifest: String): PersistentRepr = copy(manifest = manifest)

  override def update(sequenceNr: Long, persistenceId: String, deleted: Boolean, sender: ActorRef, writerUuid: String): PersistentRepr = {
    copy(sequenceNr = sequenceNr, persistenceId = persistenceId, deleted = deleted, sender = sender, writerUuid = writerUuid)
  }
}
