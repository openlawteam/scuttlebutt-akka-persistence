package org.openlaw.scuttlebutt.persistence

import com.fasterxml.jackson.annotation.JsonProperty

/**
  * Each published scuttlebutt message must have a 'type' field. The 'persistenceId' field in
  * PersistentRepr which we store and recover is a good mapping to this field, so we configure
  * our serializer to serialize this back and forward
  */
abstract class ScuttlebuttPersistentReprSerializationMixIn {

  @JsonProperty("type")
  def getPersistenceId(): String

}
