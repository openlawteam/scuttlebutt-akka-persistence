package org.openlaw.scuttlebutt.persistence.model

/**
  * These messages can be persisted as akka persistence framework persistence events
  * to encrypt content for a given entity and restrict access to only specified users.
  */

/**
  * A aes-256-ctr encryption key.
  *
  * @param key a base 64 representation of the encryption key (256 bytes)
  * @param nonce a base 64 representation of the nonce
  */
case class Key(key: String, nonce: String)

/**
  * Changes or sets the key for the persistence ID that this message is persisted for.
  *
  * @param key
  */
case class UpdateKey(key: Key)

/**
  *
  *
  * @param userId the user to allow access of the entity.
  */
case class AllowAccess(userId: String)

/**
  *
  * @param userId
  * @param newKey
  */
case class RemoveAccess(userId: String, newKey: Key)

object KeyUtils {

  def makeRandomKey() : Key = {
    null
  }

}


