package org.openlaw.scuttlebutt.persistence

import akka.actor._
import akka.persistence.{PersistentActor, _}

case class Cmd(data: String)

case class Evt(data: String)

case class ExampleState(events: List[String] = Nil) {
  def updated(evt: Evt): ExampleState = copy(evt.data :: events)
  def size: Int = events.length
  override def toString: String = events.reverse.toString
}

class ScuttlebuttPersistentActorExample extends PersistentActor {
  override def persistenceId = "sample-id-4"

  var state = ExampleState()

  def updateState(event: Evt): Unit =
    state = state.updated(event)

  def numEvents =
    state.size

  val receiveRecover: Receive = {
    case evt: Evt                                 => updateState(evt)
    case SnapshotOffer(_, snapshot: ExampleState) => state = snapshot
    case x => println("Unrecognised: " + x)
  }

  val snapShotInterval = 1000
  val receiveCommand: Receive = {
    case Cmd(data) =>
      persist(Evt(s"${data}-${numEvents}")) { event =>
        updateState(event)
        context.system.eventStream.publish(event)
        if (lastSequenceNr % snapShotInterval == 0 && lastSequenceNr != 0)
          saveSnapshot(state)
      }
    case "print" => println(state)
  }

}

object ScuttlebuttPersistentActorExampleTest {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("HelloSystem")
    // default Actor constructor
    val helloActor = system.actorOf(Props[ScuttlebuttPersistentActorExample], name = "persist-test-actor")

    helloActor ! Cmd("Test")

    helloActor ! "print"
  }

}

