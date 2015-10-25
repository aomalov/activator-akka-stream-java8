package sample.stream;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.japi.Creator;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.actor.ActorPublisherMessage;
import scala.Tuple2;
import scala.concurrent.Promise;
import scala.runtime.BoxedUnit;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Created by andrewm on 10/25/2015.
 */
public class AckedPublisherStopper extends AbstractActor {

    private CompletableFuture<Void> cfCompletionFlag;

    public static Props props(ActorRef ackedPub) {
        return Props.create(new Creator<AckedPublisherStopper>() {

        @Override
        public AckedPublisherStopper create() throws Exception {
            return new AckedPublisherStopper(ackedPub);
        }
    });
    }

    public AckedPublisherStopper(ActorRef ackedPub) {
        cfCompletionFlag=new CompletableFuture<>();

        receive(ReceiveBuilder.
                matchEquals("Stop", ev -> {
                    System.out.println("Stopping the PUBLISHER [from Stopper Actor]");
                    context().watch(ackedPub);
                    ackedPub.tell("Stop",ActorRef.noSender());
                    sender().tell((CompletionStage<Void>)cfCompletionFlag,self());
                }).
                match(Terminated.class, ev -> {
                    System.out.println("%%%% Publisher terminated [from Stopper]"+ev);
                    cfCompletionFlag.complete(null);
                    context().stop(self());
                }).
                build());
    }
}
