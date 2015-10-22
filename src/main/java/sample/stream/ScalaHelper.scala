package sample.stream

import akka.actor.ActorRef
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.{Source, RunnableGraph}
import com.timcharper.acked.{AckedSink, AckTup, AckedSourceMagnet, AckedSource}
import scala.concurrent.ExecutionContext

import scala.concurrent.Promise


/**
 * Created by andrewm on 10/21/2015.
 */
object ScalaHelper {

  def javaPublisherAckedSource(pub:  ActorRef): AckedSource[String, Unit] = {
    //TODO do some transformation with the actor
    //val r=AckedSource(AckedSourceMagnet.fromPromiseSource(pub)) //.mapMaterializedValue(ref->)
    val r=AckedSource(Source(ActorPublisher[AckTup[String]](pub)))
    r
  }

  def createAckTup(implicit ec: ExecutionContext,str:String): (Promise[Unit],String) = {

    val p = Promise[Unit]
    p.future.onComplete { v =>
      println(s"${str} acked (${v})")
    }
    (p,str)
  }
}