package sample.stream;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.actor.AbstractActorSubscriber;
import akka.stream.actor.ActorSubscriberMessage;
import akka.stream.actor.RequestStrategy;
import akka.stream.actor.WatermarkRequestStrategy;

import static akka.stream.actor.ActorSubscriberMessage.*;


/**
 * Created by andrewm on 10/20/2015.
 */
public class ActorAckedSubscriberTest extends AbstractActorSubscriber {
    public ActorAckedSubscriberTest() {
        receive(ReceiveBuilder.
                match(OnNext.class, on -> on.element() instanceof AckedFlowMsg,
                        onNext -> {
                            AckedFlowMsg<String> msg = (AckedFlowMsg<String>) onNext.element(); //type casting the message
                            System.out.println("Recieved at SINK " + msg.msgData);
                            msg.ackPromise.complete(msg.msgData);
                        }).
                match(OnError.class, err -> {
                    System.out.println("Error at subscriber " + err);
                    context().stop(self());
                }).
                match(OnComplete$.class, ev -> {
                    System.out.println("flow completed");
                    context().stop(self());
                }).
                build());
    }

    @Override
    public RequestStrategy requestStrategy() {
        return new WatermarkRequestStrategy(50);
    }

    public static Props props() { return Props.create(ActorAckedSubscriberTest.class); }


}
