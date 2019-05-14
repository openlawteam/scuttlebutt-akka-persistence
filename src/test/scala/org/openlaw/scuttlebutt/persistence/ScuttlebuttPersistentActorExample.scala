package org.openlaw.scuttlebutt.persistence

import akka.actor._
import akka.persistence.{PersistentActor, _}

class ScuttlebuttPersistentActorExample extends PersistentActor {
  override def persistenceId = "sample-id-7"

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

    helloActor ! "print"

    var i = 0
    while (i < 121) {
      helloActor ! Cmd("new-test")
      helloActor ! "print"
      i = i + 1
    }
  }

}

