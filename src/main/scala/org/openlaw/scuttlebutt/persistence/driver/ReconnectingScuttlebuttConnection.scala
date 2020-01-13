package org.openlaw.scuttlebutt.persistence.driver

import akka.NotUsed
import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.stream.scaladsl.{Source}
import akka.util.Timeout
import com.typesafe.config.Config
import org.apache.tuweni.scuttlebutt.rpc.{RPCAsyncRequest, RPCResponse, RPCStreamRequest}

import scala.concurrent.Future

/**
  * A layer over the scuttlebutt driver which intercepts broken connection exceptions and continually attempts to reconnect
  * to the scuttlebutt backend. Calling these methods will continue to raise a 'ConnectionClosedException' until the
  * connection is re-established.
  *
  * @param system              the akka system
  * @param config              the akka config
  * @param timeoutRequestAfter the time after which requests should time out if they're not serviced by the actor in time
  */
case class ReconnectingScuttlebuttConnection(system: ActorSystem,
                                             config: Config,
                                             timeoutRequestAfter: Timeout) {

  val scuttlebuttDriverActor = system.actorOf(Props(classOf[ScuttlebuttDriverActor], config))

  /**
    * Makes an async scuttlebutt RPC request. If the call fails due to the connection being closed, it attempts
    * to re-establish the connection. If a reconnection attempt is still in progress, this method will continue to
    * return a failed future (with an ConnectionClosedException exception.) until the connection has been successfully
    * re-established.
    *
    * @param request the request details
    * @return the response
    */
  def makeAsyncRequest(request: RPCAsyncRequest): Future[RPCResponse] = {
    ask(scuttlebuttDriverActor, request)(timeoutRequestAfter).mapTo[RPCResponse]
  }

  /**
    * If the call fails due to the connection being closed, it attempts
    * to re-establish the connection. If a reconnection attempt is still in progress, this method will continue to
    * return a failed future (with an ConnectionClosedException exception.) until the connection has been successfully
    * re-established.
    *
    * @param request the request details
    * @return the stream source representation (not yet open.)
    */
  def openStream(request: RPCStreamRequest): Source[RPCResponse, NotUsed] = {
    val stream = ask(scuttlebuttDriverActor, request)(timeoutRequestAfter).mapTo[Source[RPCResponse, NotUsed]]

    Source.fromFuture(stream)
      .flatMapConcat(identity)
  }

}
