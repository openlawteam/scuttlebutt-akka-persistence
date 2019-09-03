package org.openlaw.scuttlebutt.persistence.driver

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.util
import java.util.Scanner

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.tuweni.bytes.Bytes
import org.apache.tuweni.crypto.sodium.Signature
import org.apache.tuweni.io.Base64

object KeyUtils {

  def getKeysAtPath(path: String): Option[Signature.KeyPair] = {
    val secretPath: String = path + "/secret"
    val file: File = new File(secretPath)

    if (!file.exists) None
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

      val mapper: ObjectMapper = new ObjectMapper

      val values: util.Map[String, String] = mapper.readValue(secretJSON, new TypeReference[util.Map[String, String]]() {})
      val pubKey: String = values.get("public").replace(".ed25519", "")
      val privateKey: String = values.get("private").replace(".ed25519", "")

      val pubKeyBytes: Bytes = Base64.decode(pubKey)
      val privateKeyBytes: Bytes = Base64.decode(privateKey)

      val pub: Signature.PublicKey = Signature.PublicKey.fromBytes(pubKeyBytes)
      val secretKey: Signature.SecretKey = Signature.SecretKey.fromBytes(privateKeyBytes)

      return Some(new Signature.KeyPair(pub, secretKey))
    }
  }

}
