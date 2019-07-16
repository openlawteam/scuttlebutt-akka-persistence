package org.openlaw.scuttlebutt.persistence.serialization

import akka.actor.ActorSystem
import akka.serialization.{Serialization, SerializationExtension}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.reflect.ClassTag
import scala.util.Try

class ScuttlebuttPersistenceSerializer(actorSystem: ActorSystem) {

  val serialization: Serialization = SerializationExtension(actorSystem)

  def serialize(payload: AnyRef): Try[Array[Byte]] = {
    serialization.serialize(payload)
  }

  def deserialize(bytes: Array[Byte], classFqn: String): Try[Any] = {

    val clazz: Class[_] = serialization.system.dynamicAccess.getClassFor(classFqn)
      .getOrElse(Class.forName(classFqn, true, Thread.currentThread.getContextClassLoader))

    serialization.deserialize(bytes, clazz)
  }

  def getObjectMapper(): ObjectMapper = {
    val mapper: ObjectMapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
    mapper.registerModule(new JavaTimeModule)
  }

}
