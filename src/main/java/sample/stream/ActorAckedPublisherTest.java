package sample.stream;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.actor.AbstractActorPublisher;
import akka.stream.actor.ActorPublisherMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by andrewm on 10/20/2015.
 */
public class ActorAckedPublisherTest extends AbstractActorPublisher<AckedFlowMsg<String>> {
    public static Props props() { return Props.create(ActorAckedPublisherTest.class); }

    private final int MAX_BUFFER_SIZE = 8;
    private final List<AckedFlowMsg<String>> buf = new ArrayList<>();

    public ActorAckedPublisherTest () {
        receive(ReceiveBuilder.
                match(ActorPublisherMessage.Request.class, request -> {
                    System.out.println("deliver on request if something in buffer  [" + buf.size() + "] for demanded :" + totalDemand());
                    deliverBuf();
                }).
                match(ActorPublisherMessage.Cancel.class, cancel -> context().stop(self())).
                matchEquals("Stop",ev -> {
                   onCompleteThenStop();
                }).
                match(String.class, msgStr -> {
                    System.out.println("  msg accepted to flow [" + msgStr + "] while  demanded: " + totalDemand());
                    AckedFlowMsg<String> msg = new AckedFlowMsg<String>(msgStr);
                    sender().tell(msg.ackPromise, self()); //return the CompletableFuture
                    if (buf.isEmpty() && totalDemand() > 0) {
                        System.out.println("deliver without buf - " + msg.msgData);
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
                final List<AckedFlowMsg<String>> took =
                        buf.subList(0, Math.min(buf.size(), (int) totalDemand()));
                took.forEach(this::onNext);
                buf.removeAll(took);
                System.out.println("buffer size after delivery "+buf.size() + " demanded yet " + totalDemand());
                break;
            } else {
                final List<AckedFlowMsg<String>> took =
                        buf.subList(0, Math.min(buf.size(), Integer.MAX_VALUE));
                took.forEach(this::onNext);
                buf.removeAll(took);
            }
        }
    }
}
