package sample.stream;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.actor.*;
import scala.Tuple2;
import scala.concurrent.Promise;
import scala.runtime.BoxedUnit;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by andrewm on 11/10/2015.
 */
public class ActorAckedTransformerTest extends AbstractTransformerActor<Tuple2<Promise<BoxedUnit>,String>> {

    private final Queue<Tuple2<Promise<BoxedUnit>,String>> _queBuf;
    private final String _name;

    public ActorAckedTransformerTest(String name) {
        _queBuf = new LinkedList<>();
        _name=name;

        receive(ReceiveBuilder.
                match(ActorPublisherMessage.Request.class, request -> {
                    System.out.println("["+_name+"]: deliver on request if something in buffer  [" + _queBuf.size() + "] for demanded :" + totalDemand());
                    deliverBuf();
                }).
                match(ActorPublisherMessage.Cancel.class, cancel -> {
                    System.out.printf("flow canceled");
                    cancel(); //Forward the call
                    context().stop(self());
                }).
                match(ActorSubscriberMessage.OnNext.class, on -> on.element() instanceof Tuple2,
                        onNext -> {
                            Tuple2<Promise<BoxedUnit>, String> msg = (Tuple2<Promise<BoxedUnit>, String>) onNext.element(); //type casting the message
                            System.out.println("["+_name+"]:  msg accepted to flow [" + msg._2() + "] while  demanded: " + totalDemand());
                            //DO some changes
                            Tuple2<Promise<BoxedUnit>,String> newMsg=Tuple2.apply(msg._1(),msg._2()+" | changed from "+_name);
                            _queBuf.add(newMsg);
                            deliverBuf();
                        }).
                match(ActorSubscriberMessage.OnError.class, err -> {
                    System.out.println("["+_name+"]:  Error at subscriber " + err);
                    onErrorThenStop(err.cause());
                    context().stop(self());
                }).
                match(ActorSubscriberMessage.OnComplete$.class, ev -> {
                    System.out.println("["+_name+"]:   flow completed");
                    onCompleteThenStop();
                }).
                build());
    }

    public void deliverBuf(){
        while (totalDemand() > 0 && (_queBuf.peek()!=null)) {
            onNext(_queBuf.remove());
        }
    }

    public static Props props() { return Props.create(AbstractTransformerActor.class); }


    @Override
    public RequestStrategy requestStrategy() {
        return new WatermarkRequestStrategy(50);
    }

}
