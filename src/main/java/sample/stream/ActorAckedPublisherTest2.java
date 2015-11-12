package sample.stream;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.actor.AbstractActorPublisher;
import akka.stream.actor.ActorPublisherMessage;
import scala.Tuple2;
import scala.compat.java8.FutureConverters;
import scala.concurrent.Promise;
import scala.runtime.BoxedUnit;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by andrewm on 10/20/2015.
 */
public class ActorAckedPublisherTest2 extends AbstractActorPublisher<Tuple2<Promise<BoxedUnit>,String>> {
    public static Props props() { return Props.create(ActorAckedPublisherTest2.class); }

    private final List<Tuple2<Promise<BoxedUnit>,String>> buf = new ArrayList<>();

    public ActorAckedPublisherTest2() {
        receive(ReceiveBuilder.
                match(ActorPublisherMessage.Request.class, request -> {
                    System.out.println("deliver on request if something in buffer  [" + buf.size() + "] for demanded :" + totalDemand());
                    deliverBuf();
                }).
                match(ActorPublisherMessage.Cancel.class, cancel -> context().stop(self())).
                matchEquals("Stop", ev -> {
                    System.out.println("Stopping the PUBLISHER [recieved stop signal in Pub]");
                    onCompleteThenStop();
                    context().stop(self());
                }).
                match(String.class, msgStr -> {
                    System.out.println("  msg accepted to flow [" + msgStr + "] while  demanded: " + totalDemand());
                    Tuple2<Promise<BoxedUnit>, String> msg = ScalaHelper.createAckTup(context().dispatcher(), msgStr);
                    if (sender() != context().system().deadLetters()) {
                        //sender().tell(msg._1().future(), self()); //return the Future to wait on
                        sender().tell(FutureConverters.toJava(msg._1().future()), self());
                        System.out.println("Sending SCALA Future on [" + msgStr + "] /" +msg._1().future().isCompleted()+"/ back to " + sender());
                    }
                    if (buf.isEmpty() && totalDemand() > 0) {
                        System.out.println("deliver without buf - " + msg._2());
                        onNext(msg);
                    } else {
                        System.out.println("delivering with initial buffer of " + buf.size());
                        buf.add(msg);
                        deliverBuf();
                    }
                }).
                build());
    }

    public void deliverBuf(){
        while (totalDemand() > 0 && buf.size()>0) {
            if (totalDemand() <= Integer.MAX_VALUE) {
                System.out.println("buffer size before delivery "+buf.size() + " of demanded " + totalDemand() );
                final List<Tuple2<Promise<BoxedUnit>,String>> took =
                        buf.subList(0, Math.min(buf.size(), (int) totalDemand()));
                took.forEach(this::onNext);
                buf.removeAll(took);
                System.out.println("buffer size after delivery "+buf.size() + " demanded yet " + totalDemand());
                break;
            } else {
                final List<Tuple2<Promise<BoxedUnit>,String>> took =
                        buf.subList(0, Math.min(buf.size(), Integer.MAX_VALUE));
                took.forEach(this::onNext);
                buf.removeAll(took);
            }
        }
    }
}
