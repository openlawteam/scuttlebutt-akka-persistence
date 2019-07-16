package org.openlaw.scuttlebutt.persistence

import akka.serialization.Serializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.openlaw.scuttlebutt.persistence.model.{AllowAccess, UpdateKey}

case class Serializers(serializer: (AnyRef) => Array[Byte], deserializer: (Array[Byte]) => AnyRef)

class TestSerializer extends Serializer {

  val objectMapper: ObjectMapper = makeMapper()

  var serializers = Map[String, Serializers]()

  private def makeMapper() = {
    val mapper: ObjectMapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
    mapper.registerModule(new JavaTimeModule)
  }

  def registerClass[T](
                        classFqn: String,
                        serializer: (AnyRef) => Array[Byte],
                        deserializer: (Array[Byte]) => AnyRef) = {

    serializers = serializers.updated(classFqn, Serializers(serializer, deserializer))
  }

  def registerTestClasses() : Unit = {

    registerClass(
      classOf[Cmd].getName,
      (ref) => objectMapper.writeValueAsBytes(ref),
      (bytes) => objectMapper.readValue(bytes, classOf[Cmd])
    )

    registerClass(
      classOf[Evt].getName,
      (ref) => objectMapper.writeValueAsBytes(ref),
      (bytes) => objectMapper.readValue(bytes, classOf[Evt])
    )

    registerClass(
      classOf[UpdateKey].getName,
      (ref) => objectMapper.writeValueAsBytes(ref),
      (bytes) => objectMapper.readValue(bytes, classOf[UpdateKey])
    )

    registerClass(
      classOf[AllowAccess].getName,
      (ref) => objectMapper.writeValueAsBytes(ref),
      (bytes) => objectMapper.readValue(bytes, classOf[AllowAccess])
    )

  }

  registerTestClasses()

  override def identifier: Int = 1

  override def toBinary(o: AnyRef): Array[Byte] = {
    serializers.get(o.getClass.getName).map(_.serializer(o)) match {
      case Some(o) => o
      case None => throw new Exception("Could not find serializer for: " + o.getClass.getName)
    }
  }

  override def includeManifest: Boolean = true

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    manifest match {
      case None => throw new Exception("Manifest is required to deserialize")
      case Some(manif) => {
        val className = manif.getCanonicalName

        serializers.get(className) match {
          case None => throw new Exception("Could not find deserializer for " + className)
          case Some(x) => x.deserializer(bytes)
        }
      }
    }


  }
}
