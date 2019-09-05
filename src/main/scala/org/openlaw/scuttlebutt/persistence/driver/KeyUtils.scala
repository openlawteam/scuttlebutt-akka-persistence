package org.openlaw.scuttlebutt.persistence.driver

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.util
import java.util.Scanner
import java.util.stream.Collectors

import scala.collection.JavaConverters._
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.tuweni.crypto.sodium.Signature
import org.apache.tuweni.io.Base64

import scala.collection.mutable

object KeyUtils {

  def getKeysFromBase64(base64: String): Either[String, Signature.KeyPair] = {

    val jsonString = throwableToLeft({
      new String(Base64.decode(base64)toArray)
    }).left.map(
      ex => s"Could not decode key from base64 encoding to bytes. Reason: ${ex.getMessage}"
    )

    val commentsRemoved = jsonString.map(_.linesIterator.filter(line => !line.startsWith("#")).mkString(""))

    commentsRemoved.flatMap(fromKeyJSON)
  }

  def getKeysAtPath(secretPath: String): Either[String, Signature.KeyPair] = {
    try {
      val file: File = new File(secretPath)

      if (!file.exists) Left(s"Secret file not found at ${secretPath}")
      else {
        val s: Scanner = new Scanner(file, UTF_8.name)
        s.useDelimiter("\n")

        val list: util.ArrayList[String] = new util.ArrayList[String]
        while ( {
          s.hasNext
        }) {
          val next: String = s.next
          // Filter out the comment lines
          if (!next.startsWith("#")) list.add(next)
        }

        val secretJSON: String = String.join("", list)

        fromKeyJSON(secretJSON)
      }
    }
    catch {
      case e: Exception => Left(s"Unexpected exception while parsing secret key file: ${e.getMessage}")
    }
  }

  private def fromKeyJSON(secretJson: String): Either[String, Signature.KeyPair] = {
    val mapper: ObjectMapper = new ObjectMapper
    val values = throwableToLeft(mapper.readValue(secretJson, new TypeReference[util.Map[String, String]]() {}).asScala)
      .left.map(ex => s"Error deserializing secret JSON key contents: ${ex.getMessage}")

    val publicKey: Either[String, String] = values.flatMap(map => getPublicKey(map))
    val privateKey: Either[String, String] = values.flatMap(map => getPrivateKey(map))

    for {
      pubKeyString <- publicKey
      privateKeyString <- privateKey

      pubKeyBytes <- throwableToLeft(Base64.decode(pubKeyString)).left
        .map(ex => s"Could not decode public key from base64 string to bytes. Reason: ${ex.getMessage}")
      privKeyBytes <- throwableToLeft(Base64.decode(privateKeyString))
        .left.map(ex => s"Could not decode private key from base64 string to bytes. Reason: ${ex.getMessage}")

      pubKey <- throwableToLeft(Signature.PublicKey.fromBytes(pubKeyBytes))
        .left.map(ex => s"Could not create public key from public key bytes. Reason: ${ex.getMessage}")
      privKey <- throwableToLeft(Signature.SecretKey.fromBytes(privKeyBytes))
        .left.map(ex => s"Could not create private key from private key bytes. Reason:  ${ex.getMessage}.")
    } yield {
      new Signature.KeyPair(pubKey, privKey)
    }
  }

  private def getPublicKey(map: mutable.Map[String, String]): Either[String, String] = {
    map.get("public").map(_.replace(".ed25519", "")) match {
      case None => Left("Public key not found in key JSON")
      case Some(key) => Right(key)
    }
  }

  private def getPrivateKey(map: mutable.Map[String, String]): Either[String, String] = {
    map.get("private").map(_.replace(".ed25519", "")) match {
      case None => Left("Private key not found in key JSON")
      case Some(key) => Right(key)
    }
  }

  private def optionToEither[E, T](option: Option[T], left: E): Either[E, T] = {
    option match {
      case None => Left(left)
      case Some(value) => Right(value)
    }
  }

  private def throwableToLeft[T](block: => T): Either[java.lang.Throwable, T] =
    try {
      Right(block)
    } catch {
      case ex: Throwable => Left(ex)
    }

}
