package org.openlaw.scuttlebutt.persistence.serialization

import akka.persistence.PersistentRepr
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.openlaw.scuttlebutt.persistence.ScuttlebuttPersistentReprSerializationMixIn



class ScuttlebuttPersistenceSerializationConfig() {

  val mapper: ObjectMapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)


  def getObjectMapper() = {
    mapper.addMixIn(classOf[PersistentRepr], classOf[ScuttlebuttPersistentReprSerializationMixIn] )
  }

}
