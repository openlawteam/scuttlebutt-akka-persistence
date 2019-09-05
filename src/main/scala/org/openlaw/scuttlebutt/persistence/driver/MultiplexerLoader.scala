package org.openlaw.scuttlebutt.persistence.driver

import java.io.{BufferedWriter, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.function.Consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.Config
import io.vertx.core.Vertx
import org.apache.tuweni.bytes.{Bytes, Bytes32}
import org.apache.tuweni.concurrent.AsyncResult
import org.apache.tuweni.crypto.sodium.Signature
import org.apache.tuweni.io.Base64
import org.apache.tuweni.scuttlebutt.handshake.vertx.{ClientHandler, SecureScuttlebuttVertxClient}
import org.apache.tuweni.scuttlebutt.rpc.mux.RPCHandler
import org.logl.Level
import org.logl.logl.SimpleLogger


class MultiplexerLoader(objectMapper: ObjectMapper, scuttlebuttConf: Config) {

  /**
    * Connects to the configured scuttlebutt node as a client
    *
    * This function blocks until the connection is complete and the handshake finished.
    *
    * @return the rpc handler to perform requests with
    */
  def loadMultiplexer: Either[String, RPCHandler] = {

    for {
      localKeys <- getKeys()
      networkKeyBytes32 <- getNetworkKey()

      host = scuttlebuttConf.getString("host")
      port = scuttlebuttConf.getInt("port")

      debugEnabled = scuttlebuttConf.getBoolean("debug")
      debugLevel = if (debugEnabled) Level.DEBUG else Level.INFO

      vertx = Vertx.vertx()
      loggerProvider = SimpleLogger.withLogLevel(debugLevel).toPrintWriter(new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out, UTF_8))))
      secureScuttlebuttVertxClient = new SecureScuttlebuttVertxClient(loggerProvider, vertx, localKeys, networkKeyBytes32)

      onConnect: AsyncResult[RPCHandler] = secureScuttlebuttVertxClient.connectTo(port, host, localKeys.publicKey, (sender: Consumer[Bytes], terminationFn: Runnable) => {
        def makeHandler(sender: Consumer[Bytes], terminationFn: Runnable) = new RPCHandler(vertx, sender, terminationFn, objectMapper, loggerProvider)

        makeHandler(sender, terminationFn)
      })

      clientHandler <-getClientHandler(onConnect)
    } yield {
      clientHandler
    }
  }

  private def getClientHandler(asyncResult: AsyncResult[RPCHandler]): Either[String, RPCHandler] = {
    try {
      Right(asyncResult.get())
    } catch {
      case ex: Throwable => Left(ex.getMessage)
    }
  }


  private def getNetworkKey(): Either[String, Bytes32] = {
    if (!scuttlebuttConf.hasPath("networkKey")) {
      Left("No networkKey param configured.")
    } else {
      val networkKeyBase64 = scuttlebuttConf.getString("networkKey")

      try {
        Right(Bytes32.wrap(Base64.decode(networkKeyBase64)))
      }
      catch {
        case ex: Throwable => Left(s"Could not decode scuttlebutt network key from base64 to bytes. Reason: ${ex.getMessage}")
      }
    }
  }

  private def getKeys(): Either[String, Signature.KeyPair] = {
    val secretBase64ConfigPath = "secret.base64"
    val secretKeyFileConfigPath = "secret.path"

    if (scuttlebuttConf.hasPath(secretBase64ConfigPath) && scuttlebuttConf.hasPath(secretKeyFileConfigPath)) {
      val error = "secret.path and secret.base64 configuration values are both defined. Only one value should be defined."
      Left(error)
    } else if (scuttlebuttConf.hasPath(secretBase64ConfigPath)) {
      KeyUtils.getKeysFromBase64(scuttlebuttConf.getString(secretBase64ConfigPath))
    }
    else if (scuttlebuttConf.hasPath(secretKeyFileConfigPath)) {
      KeyUtils.getKeysAtPath(scuttlebuttConf.getString(secretKeyFileConfigPath))
    }
    else {
      return Left("Could not find scuttlebutt keys at the configured ssb persistence directory (checked keys.path and keys.base64 settings and the SSB_KEYPATH and SSB_SECRET_KEYPAIR environment variables.")
    }
  }

}
