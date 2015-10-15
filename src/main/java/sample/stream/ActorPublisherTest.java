package sample.stream;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.Creator;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.ActorMaterializer;
import akka.stream.actor.AbstractActorPublisher;
import akka.stream.actor.ActorPublisherMessage;
import akka.stream.impl.ActorPublisher;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by andrewm on 10/14/2015.
 */
public class ActorPublisherTest {


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


        private final int MAX_BUFFER_SIZE = 3;
        private final List<JobManagerProtocol.Job> buf = new ArrayList<>();

        public JobManager() {
            receive(ReceiveBuilder.
                    match(JobManagerProtocol.Job.class, job -> buf.size() == MAX_BUFFER_SIZE, job -> {
                        //if(sender()!=ActorRef.noSender())  sender().tell(JobManagerProtocol.JobDenied, self());
                        //sender().path().
                        System.out.println(sender()+" XXXX_  Job denied " + job.payload);
                        Thread.sleep(500);
                    }).
                    match(JobManagerProtocol.Job.class, job -> {
                        //if(!sender().equals(ActorRef.noSender())) sender().tell(JobManagerProtocol.JobAccepted, self());
                        System.out.println(sender()+"  A_C_K Job accepted " + job.payload);
                        Thread.sleep(500);

                        if (buf.isEmpty() && totalDemand() > 0) {
                            System.out.println("deliver without buf - " + job.payload);
                            onNext(job);
                        }
                        else {
                            buf.add(job);
                            deliverBuf();
                        }
                    }).
                    match(ActorPublisherMessage.Request.class, request -> {
                        System.out.println("deliver on request if something in buffer - " + buf.size());
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
                    System.out.println("buffer size after delivery "+buf.size());
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

    public static void main(String[] args) throws InterruptedException {

        final ActorSystem system = ActorSystem.create("Sys");
        final ActorMaterializer mat = ActorMaterializer.create(system);
        //final ActorRef pub = system.actorOf(JobManager.props());


        final Source<JobManagerProtocol.Job, ActorRef> jobManagerSource = Source.actorPublisher(JobManager.props());

        final ActorRef ref = jobManagerSource
                .map(job -> job.payload.toUpperCase())
                .map(elem -> {
                    System.out.println(elem);
                    return elem;
                })
                .to(Sink.ignore())
                .run(mat);

        ref.tell(new JobManagerProtocol.Job("a"), ActorRef.noSender());
        ref.tell(new JobManagerProtocol.Job("b"), ActorRef.noSender());
        ref.tell(new JobManagerProtocol.Job("c"), ActorRef.noSender());
        //Thread.sleep(1000);
        ref.tell(new JobManagerProtocol.Job("d"), ActorRef.noSender());
        Thread.sleep(2000);
        ref.tell(new JobManagerProtocol.Job("e"), ActorRef.noSender());
        ref.tell(new JobManagerProtocol.Job("f"), ActorRef.noSender());
        ref.tell(new JobManagerProtocol.Job("g"), ActorRef.noSender());
        ref.tell(new JobManagerProtocol.Job("h"), ActorRef.noSender());
        ref.tell(new JobManagerProtocol.Job("i"), ActorRef.noSender());
        ref.tell(new JobManagerProtocol.Job("j"), ActorRef.noSender());
    }
}