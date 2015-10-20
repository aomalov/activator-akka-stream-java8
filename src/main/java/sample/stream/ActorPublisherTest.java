package sample.stream;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.ActorMaterializer;
import akka.stream.actor.AbstractActorPublisher;
import akka.stream.actor.ActorPublisherMessage;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.Timeout;
import org.reactivestreams.Publisher;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import scala.reflect.ClassTag$;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by andrewm on 10/14/2015.
 */
public class ActorPublisherTest implements  ICtrlFlowPeer {
    
    private static  ActorRef refPublisherActor;
    
    @Override
    public boolean onSyncMessage(byte[] message) {
        refPublisherActor.tell(new JobManagerProtocol.Job(message.toString()),ActorRef.noSender());
        return true;
    }

    @Override
    public void onAsyncMessage(byte[] message, Future<?> future) {
        //TODO Adapt to Acked Source - but follow Ack propagation just only if Async is required
        throw new NotImplementedException();
    }

    public static class JobManagerProtocol {
        final public static class Job {
            public final String payload;

            public Job(String payload) {
                this.payload = payload;
            }

        }

        public static class JobAcceptedMessage {
            @Override
            public String toString() {
                return "JobAccepted";
            }
        }
        public static final JobAcceptedMessage JobAccepted = new JobAcceptedMessage();

        public static class JobDeniedMessage {
            @Override
            public String toString() {
                return "JobDenied";
            }
        }
        public static final JobDeniedMessage JobDenied = new JobDeniedMessage();
    }

    public static class JobManager extends AbstractActorPublisher<JobManagerProtocol.Job> {

        public static Props props() { return Props.create(JobManager.class); }
//        {
//            return Props.create(new Creator<JobManager>() {
//                private static final long serialVersionUID = 2L;
//
//                @Override
//                public JobManager create() throws Exception {
//                    return new JobManager();
//                }
//            });
//        }


        private final int MAX_BUFFER_SIZE = 8;
        private final List<JobManagerProtocol.Job> buf = new ArrayList<>();

        public JobManager() {
            receive(ReceiveBuilder.
                    match(JobManagerProtocol.Job.class, job -> buf.size() == MAX_BUFFER_SIZE, job -> {
                        //if(sender()!=ActorRef.noSender())  sender().tell(JobManagerProtocol.JobDenied, self());
                        System.out.println(sender()+" XXXX_  Job denied " + job.payload);
                        //Thread.sleep(500);
                    }).
                    match(JobManagerProtocol.Job.class, job -> {
                        //if(!sender().equals(ActorRef.noSender())) sender().tell(JobManagerProtocol.JobAccepted, self());
                        System.out.println(sender()+"  A_C_K Job accepted " + job.payload+" demanded: "+ totalDemand());
                        //Thread.sleep(500);

                        if (buf.isEmpty() && totalDemand() > 0) {
                            System.out.println("deliver without buf - " + job.payload);
                            onNext(job);
                        }
                        else {
                            System.out.println("delivering with buffer of "+buf.size());
                            buf.add(job);
                            deliverBuf();
                        }
                        //if(totalDemand() > 0) onNext(job);
                    }).
                    match(ActorPublisherMessage.Request.class, request -> {
                        System.out.println("deliver on request if something in buffer - " + buf.size()+" demanded :"+totalDemand());
                        deliverBuf();
                    }).
                    match(ActorPublisherMessage.Cancel.class, cancel -> context().stop(self())).
                    build());
        }

        void deliverBuf() {
            while (totalDemand() > 0 && buf.size()>0) {
        /*
         * totalDemand is a Long and could be larger than
         * what buf.splitAt can accept
         */
                if (totalDemand() <= Integer.MAX_VALUE) {
                    System.out.println("buffer size before delivery "+buf.size() + " of demanded " + totalDemand() );
                    final List<JobManagerProtocol.Job> took =
                            buf.subList(0, Math.min(buf.size(), (int) totalDemand()));
                    took.forEach(this::onNext);
                    buf.removeAll(took);
                    System.out.println("buffer size after delivery "+buf.size() + " demanded yet " + totalDemand());
                    break;
                } else {
                    final List<JobManagerProtocol.Job> took =
                            buf.subList(0, Math.min(buf.size(), Integer.MAX_VALUE));
                    took.forEach(this::onNext);
                    buf.removeAll(took);
                }
            }
        }
    }

    public static void testGenericActors() throws InterruptedException {
        final ActorSystem system = ActorSystem.create("Sys");
        final ActorMaterializer mat = ActorMaterializer.create(system);
        final ActorRef pub = system.actorOf(JobManager.props());

        Publisher<JobManagerProtocol.Job> iPub = AbstractActorPublisher.create(pub);
        refPublisherActor=pub;
        refPublisherActor.tell(new JobManagerProtocol.Job("0000000a"), ActorRef.noSender());  // Works even before the flow is closed and materialized :))

        //final Source<JobManagerProtocol.Job, ActorRef> jobManagerSource = Source.actorPublisher(JobManager.props());
        Source.from(iPub)
                //refPublisherActor =
                // jobManagerSource
                //.buffer(6,OverflowStrategy.backpressure())
                .map(job -> {
                    Thread.sleep(500);
                    return job.payload.toUpperCase();
                })
                .map(elem -> {
                    //Thread.sleep(1000);
                    System.out.println(elem);
                    return elem;
                })
                .to(Sink.ignore())
                .run(mat);

        //Refer to the Publisher Actor and send it a Msg


        refPublisherActor.tell(new JobManagerProtocol.Job("a"), ActorRef.noSender());
        refPublisherActor.tell(new JobManagerProtocol.Job("b"), ActorRef.noSender());
        refPublisherActor.tell(new JobManagerProtocol.Job("c"), ActorRef.noSender());
        //Thread.sleep(1000);
        refPublisherActor.tell(new JobManagerProtocol.Job("d"), ActorRef.noSender());
        //Thread.sleep(2000);
        refPublisherActor.tell(new JobManagerProtocol.Job("e"), ActorRef.noSender());
        refPublisherActor.tell(new JobManagerProtocol.Job("f"), ActorRef.noSender());
        refPublisherActor.tell(new JobManagerProtocol.Job("g"), ActorRef.noSender());
        refPublisherActor.tell(new JobManagerProtocol.Job("h"), ActorRef.noSender());
        refPublisherActor.tell(new JobManagerProtocol.Job("i"), ActorRef.noSender());
        refPublisherActor.tell(new JobManagerProtocol.Job("j"), ActorRef.noSender());
        Thread.sleep(5000);
        refPublisherActor.tell(new JobManagerProtocol.Job("kkk"), ActorRef.noSender());
    }

    static void testAckedActors() throws Exception {
        final ActorSystem system = ActorSystem.create("Sys");
        final ActorMaterializer mat = ActorMaterializer.create(system);
        final ActorRef ackedMat = system.actorOf(ActorAckedPublisherTest.props());
        final Publisher<AckedFlowMsg<String>> iPub = AbstractActorPublisher.create(ackedMat);

        Source.from(iPub).runWith(Sink.actorSubscriber((ActorAckedSubscriberTest.props())), mat);

        //System.out.println(ackedMat);
        scala.concurrent.Future<CompletableFuture<String>> future = Patterns.ask(ackedMat, "test", 1000).mapTo(ClassTag$.MODULE$.apply(CompletableFuture.class));
        //Wait to get the CF to acknowledgement promise
        CompletableFuture<String> response = Await.result(future, new FiniteDuration(2, TimeUnit.SECONDS));
        //Wait till ack is completed actually
        String result=response.get(2,TimeUnit.SECONDS);
        System.out.println("Got the ACK completion result: "+result);

        for(int k=0;k<100;k++){
            future = Patterns.ask(ackedMat, "test_" + (k + 1), new Timeout(100,TimeUnit.MICROSECONDS)).mapTo(ClassTag$.MODULE$.apply(CompletableFuture.class));
            response = Await.result(future, new FiniteDuration(100, TimeUnit.MICROSECONDS));
            //Wait till ack is completed actually
            result=response.get(100,TimeUnit.MICROSECONDS);
        }

        Patterns.ask(ackedMat, "Stop", 100);
        system.shutdown();

    }

    public static void main(String[] args) throws Exception {

        //TODO - Pair<CtrlFlowSubscription,CompletionStage> - to work with Acked flow


        testAckedActors();
        //testGenericActors();
    }


}