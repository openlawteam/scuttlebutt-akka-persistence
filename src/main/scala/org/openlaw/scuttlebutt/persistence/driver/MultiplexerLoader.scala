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
import org.apache.tuweni.scuttlebutt.handshake.vertx.SecureScuttlebuttVertxClient
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
  def loadMultiplexer: RPCHandler = {

    val keyPair = getKeys()

    val networkKey = scuttlebuttConf.getString("networkKey")

    // TODO: make it possible to modify the host, etc, in the akka config

    if (!keyPair.isDefined) {
      val error = "Could not find scuttlebutt keys at the configured ssb persistence directory (checked keys.path and keys.base64 settings and the SSB_KEYPATH and SSB_SECRET_KEYPAIR environment variables."
      throw new Exception(error)
    }

    val localKeys = keyPair.get

    val networkKeyBase64 = networkKey
    val networkKeyBytes32 = Bytes32.wrap(Base64.decode(networkKeyBase64))

    val host = scuttlebuttConf.getString("host")
    val port = scuttlebuttConf.getInt("port")

    val debugEnabled = scuttlebuttConf.getBoolean("debug")
    val debugLevel = if (debugEnabled) Level.DEBUG else Level.INFO

    val vertx = Vertx.vertx()
    val loggerProvider = SimpleLogger.withLogLevel(debugLevel).toPrintWriter(new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out, UTF_8))))
    val secureScuttlebuttVertxClient = new SecureScuttlebuttVertxClient(loggerProvider, vertx, localKeys, networkKeyBytes32)

    val onConnect: AsyncResult[RPCHandler] = secureScuttlebuttVertxClient.connectTo(port, host, localKeys.publicKey, (sender: Consumer[Bytes], terminationFn: Runnable) => {
      def makeHandler(sender: Consumer[Bytes], terminationFn: Runnable) = new RPCHandler(vertx, sender, terminationFn, objectMapper, loggerProvider)

      makeHandler(sender, terminationFn)
    })

    onConnect.get()
  }

  private def getKeys(): Option[Signature.KeyPair] = {
    val secretBase64ConfigPath = "secret.base64"
    val secretKeyFileConfigPath = "secret.path"

    if (scuttlebuttConf.hasPath(secretBase64ConfigPath) && scuttlebuttConf.hasPath(secretKeyFileConfigPath)) {
      val error = "secret.path and secret.base64 configuration values are both defined. Only one value should be defined."
      throw new Exception(error)
    } else if (scuttlebuttConf.hasPath(secretBase64ConfigPath)) {
      KeyUtils.getKeysFromBase64(scuttlebuttConf.getString(secretBase64ConfigPath))
    }
    else if (scuttlebuttConf.hasPath(secretKeyFileConfigPath)) {
      KeyUtils.getKeysAtPath(scuttlebuttConf.getString(secretKeyFileConfigPath))
    }
    else {
      return None
    }
  }

}
