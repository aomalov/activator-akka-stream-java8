package sample.stream

import java.util.Optional
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.actor.{Props, ActorLogging, AbstractActor, ActorRef}
import akka.stream.actor.{ActorSubscriber, ActorPublisher}
import akka.stream.scaladsl.{Flow, Sink, Source, RunnableGraph}
import com.timcharper.acked._
import com.traiana.ctrl.ICtrlTransformer
import scala.annotation.tailrec
import scala.collection.JavaConversions
import scala.collection.immutable.Queue
import scala.compat.java8.FutureConverters
import scala.concurrent.{Future, ExecutionContext, Promise}
import scala.runtime.BoxedUnit


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

  def javaSubscriberAckedSink(sub: ActorRef): AckedSink[String,Unit]={
    val r=AckedSink(Sink(ActorSubscriber[AckTup[String]](sub)))
    r
  }

  def createAckTup(implicit ec: ExecutionContext,str:String): (Promise[Unit],String) = {

    val p = Promise[Unit]
//    p.future.onComplete { v =>
//      println(s"${str} acked (${v})")
//    }
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

//  def setupAckFlow(transformerProps: Props)(implicit ec: ExecutionContext): AckedFlow[String, String, Unit] = {
//    //val pub=ActorPublisher[AckTup[String]](transformer)
//    AckedFlow[AckTup[String]].to(
//    AckedSink {
//      Sink.actorSubscriber[AckTup[String]](transformerProps).mapMaterializedValue(subscriber => null)
//  }


  def IterableAckedSource(iter: java.lang.Iterable[String]): AckedSource[String, Unit] = {
    val scalaIterable: List[String] = (List.newBuilder[String] ++= JavaConversions.iterableAsScalaIterable(iter)).result()
    AckedSource(AckedSourceMagnet.fromIterable[String](scalaIterable))
  }

  def setupAckFlow(tran: ICtrlTransformer)(implicit ec: ExecutionContext): AckedFlow[String, String, Unit] = {
    AckedFlow(Flow[AckTup[String]].map { case (p, v) =>
      val new_val=tran.transform(v).orElse("not defined")
      println(new_val)
      (p, new_val)
    })
  }

  def setupFilteredAckFlow(tran: ICtrlTransformer, tranName: String)(implicit ec:ExecutionContext):AckedFlow[String,String,Unit] = {
    AckedFlow(Flow[AckTup[String]].map(ackTup=>{
      println("["+tranName+"-map ] "+ackTup._2)
      (ackTup._1,tran.transform(ackTup._2))
    }).filter(ackTup=>{
      println("["+tranName+"-filter ] "+ackTup._2.orElse("is empty"))
      if(!ackTup._2.isPresent) ackTup._1.failure(new Exception("empty transformation"))
      ackTup._2.isPresent
    }).map(ackTup=>(ackTup._1,ackTup._2.get())))
  }

  def setupAckTracker()(implicit ec: ExecutionContext): AckedFlow[String, String, Unit] = {
    AckedFlow(Flow[AckTup[String]].map { case (p, m) =>

      val newPromise = Promise[Unit]()
      val printingFuture = newPromise.future.transform(v => {
        println("message acked!")
        v
      }, t => {
        println("message NACKed! ["+t.getMessage+"]")
        t
      })

      p.completeWith(printingFuture)
      (newPromise, m)
    })
  }


  def convertJavaList[T](iter: java.lang.Iterable[T]): List[T] ={
    val scalaIterable: List[T] = (List.newBuilder[T] ++= JavaConversions.iterableAsScalaIterable(iter)).result()
    scalaIterable
  }


 @tailrec def addToGraph(ackedShapeFrom: AckedSource[String,Unit], transformers:List[ICtrlTransformer], tran_no: Int, tran_max_no: Int )(implicit ec: ExecutionContext): AckedSource[String,Unit] ={
    if(tran_no<tran_max_no)  addToGraph(ackedShapeFrom.via(setupFilteredAckFlow(transformers(tran_no),"tran_"+tran_no.toString)),transformers,tran_no+1, tran_max_no)
    else ackedShapeFrom
  }

}

abstract class AbstractTransformerActor[T] extends AbstractActor with ActorPublisher[T] with ActorSubscriber with ActorLogging
