package org.openlaw.scuttlebutt.persistence

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.util
import java.util.Scanner

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import net.consensys.cava.bytes.Bytes
import net.consensys.cava.crypto.sodium.Signature
import net.consensys.cava.io.Base64

object KeyUtils {

  def getLocalKeys(): Option[Signature.KeyPair] = {

    val ssbDir: Option[String] = Option(System.getenv.get("ssb_dir"))
    val homePath: Option[String] = Option(System.getProperty("user.home")).map((home: String) => home + "/.ssb")

    val path: Option[String] = ssbDir.orElse(homePath)

    path.flatMap(keyPath => {
      val secretPath: String = path.get + "/secret"
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

        val values: util.HashMap[String, String] = mapper.readValue(secretJSON, new TypeReference[util.Map[String, String]]() {})
        val pubKey: String = values.get("public").replace(".ed25519", "")
        val privateKey: String = values.get("private").replace(".ed25519", "")

        val pubKeyBytes: Bytes = Base64.decode(pubKey)
        val privKeyBytes: Bytes = Base64.decode(privateKey)

        val pub: Signature.PublicKey = Signature.PublicKey.fromBytes(pubKeyBytes)
        val secretKey: Signature.SecretKey = Signature.SecretKey.fromBytes(privKeyBytes)

        return Some(new Signature.KeyPair(pub, secretKey))
      }


    })
  }


}
