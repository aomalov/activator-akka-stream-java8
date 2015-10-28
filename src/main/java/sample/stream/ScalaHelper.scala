package sample.stream

import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.actor.ActorRef
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.{Flow, Sink, Source, RunnableGraph}
import com.timcharper.acked._
import scala.compat.java8.FutureConverters
import scala.concurrent.{Future, ExecutionContext, Promise}


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

  def printingAckedSink(implicit ec: ExecutionContext): AckedSink[String, CompletionStage[Void]] = {
    AckedFlow[String].
      map(msg => println("[PRINTING SINK] recieved at sink "+msg)).
      toMat(AckedSink.ack)(combiner)
  }

  def ctrlAPPAckedSink(implicit ec: ExecutionContext,flowPeerAPP: ICtrlFlowPeer[String]): AckedSink[String, CompletionStage[Void]] = {
    AckedFlow[String].
      map(msg => {
        flowPeerAPP.onNext(msg,null)
      }).
      toMat(AckedSink.ack)(combiner)
    //AckedSink.apply()
  }

  def ctrlAPPAckedSink2(implicit ec: ExecutionContext,flowPeerAPP: ICtrlFlowPeer[String]): AckedSink[String, CompletionStage[Void]] = {

    AckedSink[String,CompletionStage[Void]] {
      Sink.foreach[AckTup[String]] {
        case (p:Promise[Unit],msg:String)=> flowPeerAPP.onNext(msg,ActorPublisherTest.getCFInitialized(p))
      }.mapMaterializedValue({f=> FutureConverters.toJava(f.map(_ => null).mapTo[Void])})
    }
  }

//    Sink.foreach(case (p,data)=>{p.success(())})
//    AckedFlow[String].wrappedRepr.map {
//      case (p,msg)=>{
//        flowPeerAPP.onNext(msg.getBytes(),ActorPublisherTest.getCFInitialized(p))
//      }
//      (p,msg)
//    }.
//
//      .toMat(Sink.ignore)(combiner)
//  }


  def combiner(ignored: Any, f: Future[Unit])(implicit ec: ExecutionContext) = FutureConverters.toJava(f.map(_ => null).mapTo[Void])

  def connect[In, MI, MO](source: AckedSource[In, MI], sink: AckedSink[In, MO]):  RunnableGraph[akka.japi.Pair[MI, MO]] = {
    source.toMat(sink)(akka.japi.Pair.create)
  }
}

