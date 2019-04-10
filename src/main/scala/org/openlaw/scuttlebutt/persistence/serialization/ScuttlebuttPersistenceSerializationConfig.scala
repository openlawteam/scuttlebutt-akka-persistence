package org.openlaw.scuttlebutt.persistence.serialization

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

class ScuttlebuttPersistenceSerializationConfig() {

  val mapper: ObjectMapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)


  def getObjectMapper() = {
    mapper
  }

}
