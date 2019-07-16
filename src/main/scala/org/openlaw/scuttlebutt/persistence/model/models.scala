package org.openlaw.scuttlebutt.persistence.model

import akka.actor.ActorRef
import akka.persistence.PersistentRepr

/**
  * These messages can be persisted as akka persistence framework persistence events
  * to encrypt content for a given entity and restrict access to only specified users.
  */

/**
  * Set a new key for the persistence ID that this message is persisted for. This message
  * and all future messages will be encrypted.
  *
  * This uses aes-256 with a randomly generated 8-byte nonce under the hood. This means there is a
  * is a 50% likelihood of a collision after about 5.3B encryptions
  *
  * @param key
  */
case class UpdateKey()

/**
  * Allows a user access to the entity, sending them the current and all previous keys.
  *
  * @param userId the user to allow access of the entity.
  */
case class AllowAccess(userId: String)

/**
  * Removes a user from the access list, preventing them from seeing future updates.
  *
  * @param userId the user to remove access to future updates to the entity
  */
case class RemoveAccess(userId: String)


