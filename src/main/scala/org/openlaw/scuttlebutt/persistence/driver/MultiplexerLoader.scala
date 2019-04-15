package org.openlaw.scuttlebutt.persistence.driver

import java.io.{BufferedWriter, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.function.Consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.Config
import io.vertx.core.Vertx
import net.consensys.cava.bytes.{Bytes, Bytes32}
import net.consensys.cava.concurrent.AsyncResult
import net.consensys.cava.crypto.sodium.Signature
import net.consensys.cava.io.Base64
import net.consensys.cava.scuttlebutt.handshake.vertx.SecureScuttlebuttVertxClient
import net.consensys.cava.scuttlebutt.rpc.mux.RPCHandler
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
      throw new Exception("Could not find local scuttlebutt keys.")
    }

    val localKeys = keyPair.get

    val networkKeyBase64 = networkKey
    val networkKeyBytes32 = Bytes32.wrap(Base64.decode(networkKeyBase64))

    val host = scuttlebuttConf.getString("host")
    val port = scuttlebuttConf.getInt("port")

    val vertx = Vertx.vertx()
    val loggerProvider = SimpleLogger.withLogLevel(Level.DEBUG).toPrintWriter(new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out, UTF_8))))
    val secureScuttlebuttVertxClient = new SecureScuttlebuttVertxClient(loggerProvider, vertx, localKeys, networkKeyBytes32)

    val onConnect: AsyncResult[RPCHandler] = secureScuttlebuttVertxClient.connectTo(port, host, localKeys.publicKey, (sender: Consumer[Bytes], terminationFn: Runnable) => {
      def makeHandler(sender: Consumer[Bytes], terminationFn: Runnable) = new RPCHandler(sender, terminationFn, objectMapper, loggerProvider)

      makeHandler(sender, terminationFn)
    })

    onConnect.get()
  }

  private def getKeys(): Option[Signature.KeyPair] = {
    if (scuttlebuttConf.hasPath("secret.path")) {
      KeyUtils.getKeysAtPath(scuttlebuttConf.getString("scuttlebutt-journal.secret.path"))
    }
    else {
      KeyUtils.getLocalKeys()
    }
  }

}
