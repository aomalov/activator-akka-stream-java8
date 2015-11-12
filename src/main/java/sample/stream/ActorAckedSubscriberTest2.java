package sample.stream;

import akka.actor.Props;
import akka.japi.Pair;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.actor.AbstractActorSubscriber;
import akka.stream.actor.RequestStrategy;
import akka.stream.actor.WatermarkRequestStrategy;
import scala.Tuple2;
import scala.concurrent.Promise;
import scala.runtime.BoxedUnit;

import static akka.stream.actor.ActorSubscriberMessage.*;


/**
 * Created by andrewm on 10/20/2015.
 */
public class ActorAckedSubscriberTest2 extends AbstractActorSubscriber {
    public ActorAckedSubscriberTest2() {
        receive(ReceiveBuilder.
                match(OnNext.class, on -> on.element() instanceof Tuple2,
                        onNext -> {
                            Tuple2<Promise<BoxedUnit>, String> msg = (Tuple2<Promise<BoxedUnit>, String>) onNext.element(); //type casting the message
                            System.out.println("[Subscriber Actor] Recieved at SINK " + msg._2());
                            msg._1().success(null);
                        }).
                match(OnError.class, err -> {
                    System.out.println("Error at subscriber " + err);
                    context().stop(self());
                }).
                match(OnComplete$.class, ev -> {
                    System.out.println("[Subscriber Actor] flow completed");
                    context().stop(self());
                }).
                build());
    }

    @Override
    public RequestStrategy requestStrategy() {
        return new WatermarkRequestStrategy(50);
    }

    public static Props props() { return Props.create(ActorAckedSubscriberTest2.class); }


}
