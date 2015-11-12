package sample.stream;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.OnSuccess;
import akka.japi.Pair;
import akka.pattern.Patterns;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.ActorMaterializer;
import akka.stream.Graph;
import akka.stream.actor.AbstractActorPublisher;
import akka.stream.actor.ActorPublisherMessage;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.scaladsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.Timeout;
import com.timcharper.acked.AckedFlow;
import com.timcharper.acked.AckedSink;
import com.timcharper.acked.AckedSource;


import com.timcharper.acked.AckedSourceMagnet;
import com.traiana.ctrl.ICtrlTransformer;

import org.reactivestreams.Publisher;
import scala.Tuple2;
import scala.Unit;
import scala.compat.java8.FutureConverters;
import scala.concurrent.Await;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Promise;
import scala.concurrent.duration.FiniteDuration;
import scala.reflect.ClassTag$;
import scala.runtime.BoxedUnit;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Created by andrewm on 10/14/2015.
 */
public class ActorPublisherTest implements  ICtrlFlowPeer<String> {
    
    private static  ActorRef refPublisherActor;

    @Override
    public void onNext(String message,CompletionStage<Void> cfPromiseStage) {

        CompletableFuture<Void> cfPromise=null;
        try {
            cfPromise=cfPromiseStage.toCompletableFuture();
            //do something on the delivered message

            System.out.println("[APP] got the message "+message);
            Thread.sleep(2000);
            System.out.println("now completing the promise on sub");
            if(cfPromise!=null) cfPromise.complete(null);
        } catch (InterruptedException e) {
            if(cfPromise!=null) cfPromise.completeExceptionally(e);
            e.printStackTrace();
        }
    }


    public static class simpleSyncAppReciever extends SyncAppReceiver<String> {

        @Override
        void receive(String msg) throws Exception {
            System.out.println("[Simple APP] got the message into simple stub ["+msg+"]");
            Thread.sleep(1000); //do something
    }
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

    public static void testAckedActors() throws Exception {
        final ActorSystem system = ActorSystem.create("Sys");
        final ActorMaterializer mat = ActorMaterializer.create(system);
        final ActorRef ackedMat;
        //ActorRef ackedMat = system.actorOf(ActorAckedPublisherTest.props());
        //final Publisher<AckedFlowMsg<String>> iPub = AbstractActorPublisher.create(ackedMat);

        //Source.from(iPub).runWith(Sink.actorSubscriber((ActorAckedSubscriberTest.props())), mat);


        //Pair<BoxedUnit,ActorRef> graphMat= Source.from(iPub).toMat(Sink.actorSubscriber(ActorAckedSubscriberTest.props()), Keep.both()).run(mat);
        final Source<AckedFlowMsg<String>, ActorRef> src2 = Source.actorPublisher(ActorAckedPublisherTest.props());
        Pair<ActorRef,ActorRef> graphMat2= src2.toMat(Sink.actorSubscriber(ActorAckedSubscriberTest.props()), Keep.both()).run(mat);
        ackedMat=graphMat2.first();

        System.out.println(graphMat2.first());
        System.out.println(graphMat2.second());


        scala.concurrent.Future<CompletableFuture<String>> future = Patterns.ask(ackedMat, "test FIRST with callback", 1000).mapTo(ClassTag$.MODULE$.apply(CompletableFuture.class));
        //Wait to get the CF to acknowledgement promise
        CompletableFuture<String> response = Await.result(future, new FiniteDuration(2, TimeUnit.SECONDS));
        //Wait till ack is completed actually
        String result=response.get(2,TimeUnit.MILLISECONDS);
        System.out.println("Got the ACK completion result: "+result);

        for(int k=0;k<100;k++){
            future = Patterns.ask(ackedMat, "test_" + (k + 1), new Timeout(3000,TimeUnit.MICROSECONDS)).mapTo(ClassTag$.MODULE$.apply(CompletableFuture.class));
            response = Await.result(future, new FiniteDuration(3000, TimeUnit.MICROSECONDS));
            //Wait till ack is completed actually
            result=response.get(3000,TimeUnit.MICROSECONDS);
        }

        Patterns.ask(ackedMat, "Stop", 100);
        system.shutdown();

    }

    public static void testAckedLibSource(ICtrlFlowPeer<String> testerApp) throws Exception {
        final ActorSystem system = ActorSystem.create("Sys");
        final ActorMaterializer mat = ActorMaterializer.create(system);
        final ActorRef ackedMat = system.actorOf(ActorAckedPublisherTest2.props());
        final ActorRef refStopper = system.actorOf(AckedPublisherStopper.props(ackedMat));
        final ExecutionContext ec=system.dispatcher();

        //final Source<Tuple2<Promise<BoxedUnit>,String>, ActorRef> src2 = Source.actorPublisher(ActorAckedPublisherTest2.props());
        //AckedSource<String,ActorRef> src3= ScalaHelper.javaPublisherAckedSource(src2.asScala());


        AckedSink<String, CompletionStage<Void>> ackedSink = ScalaHelper.ctrlAPPAckedSink(system.dispatcher(),testerApp);  //.printingAckedSink(system.dispatcher());
        //AckedSink<String, CompletionStage<Void>> ackedSink = ScalaHelper.ctrlAPPAckedSink2(system.dispatcher(),testerApp);  //.printingAckedSink(system.dispatcher());

        //ScalaHelper.javaPublisherAckedSource(ackedMat).runAck(mat);  //No acked sink is needed

        Pair<BoxedUnit,CompletionStage<Void>> flowMgr= ScalaHelper.connect(ScalaHelper.javaPublisherAckedSource(ackedMat), ackedSink).run(mat);
        flowMgr.second().handle((ok, ex) -> {
            if (ok == null) {
                System.out.println("+++COMPLETION STAGE successful - " + ok);
                return 1;
            } else {
                System.out.println("++++Problem in COMPLETION STAGE-" + ex);
                return 0;
            }
        });


        ackedMat.tell("test2", ActorRef.noSender());

        //akka.dispatch.Futures.future - and also Promise is there in the Package for the sake of convenience
        scala.concurrent.Future<CompletionStage<Void>> future = Patterns.ask(ackedMat, "test with Callback", new Timeout(300, TimeUnit.MICROSECONDS))
                .mapTo(ClassTag$.MODULE$.apply(CompletionStage.class));
        //Wait to get the CF to acknowledgement promise
        CompletionStage<Void> responseFuture = Await.result(future, new FiniteDuration(200, TimeUnit.MILLISECONDS));

        responseFuture.handle((ok, ex) -> {
            if (ok == null) {
                System.out.println("GOT the [test] SUCCESS ACK hook from a Converted Future");
                //system.shutdown();
                return 1;
            } else {
                System.out.println("++++Problem in Publication STOPPING-" + ex);
                return 0;
            }
        });

        //Wait till ack is completed actually or set a hook
//        responseFuture.onSuccess(new OnSuccess<BoxedUnit>() {
//            public void onSuccess(BoxedUnit result) {
//                System.out.println("GOT the [test] SUCCESS ACK hook");
//                //ackedMat.tell("Stop",ActorRef.noSender());
//                //system.shutdown();
//            }
//        }, ec);


        Thread.sleep(9000);
        scala.concurrent.Future<CompletionStage<Void>> future2 = Patterns.ask(refStopper, "Stop", new Timeout(300, TimeUnit.MICROSECONDS))
                .mapTo(ClassTag$.MODULE$.apply(CompletionStage.class));
        CompletionStage<Void> csCompleted = Await.result(future2, new FiniteDuration(20, TimeUnit.MILLISECONDS));
        csCompleted.handle((ok, ex) -> {
            if (ok == null) {
                System.out.println("+++Publication STOPPING successful - " + ok);
                system.shutdown();
                return 1;
            } else {
                System.out.println("++++Problem in Publication STOPPING-" + ex);
                return 0;
            }
        });
    }

    public static void testAckedPubSub() throws Exception{
        final ActorSystem system = ActorSystem.create("Sys");
        final ActorMaterializer mat = ActorMaterializer.create(system);
        final ActorRef ackedMat = system.actorOf(ActorAckedPublisherTest2.props());
        final ActorRef refStopper = system.actorOf(AckedPublisherStopper.props(ackedMat));
        final ActorRef ackedSub = system.actorOf(ActorAckedSubscriberTest2.props());

        final ExecutionContext ec=system.dispatcher();

        //final Source<Tuple2<Promise<BoxedUnit>,String>, ActorRef> src2 = Source.actorPublisher(ActorAckedPublisherTest2.props());
        //AckedSource<String,ActorRef> src3= ScalaHelper.javaPublisherAckedSource(src2.asScala());


        //AckedSink<String, CompletionStage<Void>> ackedSink = ScalaHelper.ctrlAPPAckedSink(system.dispatcher(),testerApp);  //.printingAckedSink(system.dispatcher());
        //AckedSink<String, CompletionStage<Void>> ackedSink = ScalaHelper.ctrlAPPAckedSink2(system.dispatcher(),testerApp);  //.printingAckedSink(system.dispatcher());
        AckedSink<String, BoxedUnit> ackedSink = ScalaHelper.javaSubscriberAckedSink(ackedSub);

        //ScalaHelper.javaPublisherAckedSource(ackedMat).runAck(mat);  //No acked sink is needed

        ScalaHelper.connect(ScalaHelper.javaPublisherAckedSource(ackedMat), ackedSink).run(mat);

        ackedMat.tell("test2", ActorRef.noSender());

        //akka.dispatch.Futures.future - and also Promise is there in the Package for the sake of convenience
        scala.concurrent.Future<scala.concurrent.Future<BoxedUnit>> future = Patterns.ask(ackedMat, "test with Callback", new Timeout(300, TimeUnit.MICROSECONDS))
                .mapTo(ClassTag$.MODULE$.apply(scala.concurrent.Future.class));
        //Wait to get the CF to acknowledgement promise
        scala.concurrent.Future<BoxedUnit> responseFuture = Await.result(future, new FiniteDuration(20, TimeUnit.MILLISECONDS));
        //Wait till ack is completed actually or set a hook
        responseFuture.onSuccess(new OnSuccess<BoxedUnit>() {
            public void onSuccess(BoxedUnit result) {
                System.out.println("GOT the [test] SUCCESS ACK hook");
                //ackedMat.tell("Stop",ActorRef.noSender());
                //system.shutdown();
            }
        }, ec);


        Thread.sleep(5000);
        scala.concurrent.Future<CompletionStage<Void>> future2 = Patterns.ask(refStopper, "Stop", new Timeout(300, TimeUnit.MICROSECONDS))
                .mapTo(ClassTag$.MODULE$.apply(CompletionStage.class));
        CompletionStage<Void> csCompleted = Await.result(future2, new FiniteDuration(20, TimeUnit.MILLISECONDS));
        csCompleted.handle((ok, ex) -> {
            if (ok == null) {
                System.out.println("+++Publication STOPPING successful - " + ok);
                system.shutdown();
                return 1;
            } else {
                System.out.println("++++Problem in Publication STOPPING-" + ex);
                return 0;
            }
        });
    }

    public static CompletableFuture<Void> getCFInitialized(Promise<Unit> p) {
        CompletableFuture<Void> res=new CompletableFuture<>();

        res.handle((ok, ex) -> {
            if (ok == null) {
                System.out.println("**** CF completed from the APP - now ACK fulfills the promise");
                p.success(null);
            }
            else p.failure(ex);
            return null;
        });

        return res;
    }

    public static void main(String[] args) throws Exception {

        //TODO - Pair<CtrlFlowSubscription,CompletionStage> - to work with Acked flow
        //now got pair of CompletionStage

        testExternalTransformers();

    }

    public static void testExternalTransformers() throws IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException {
        final ActorSystem system = ActorSystem.create("Sys");
        final ActorMaterializer mat = ActorMaterializer.create(system);
        List<ICtrlTransformer> transformers=new ArrayList<>();


        File directory = new File("D:/Tmp/");
        if (directory.exists()) {
            // Get the list of the files contained in the package
            String[] files = directory.list(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith("jar");
                }
            });
            for (int i = 0; i < files.length; i++) {
                Manifest m = new JarFile("D:/Tmp/transformers_1.jar").getManifest();
                if (m.getEntries().size() == 1)
                    m.getEntries().forEach((k, v) -> System.out.println("[" + k + "]  " + v.getValue("Transformer") + " " + v.getValue("Version")));
            }
        }


        URLClassLoader urlcl = URLClassLoader.newInstance(new URL[] {new URL("file:///D:/Tmp/transformers_1.jar" )});
        for (String tranClass: new String[] {"com.traiana.ctrl.Transformer1","com.traiana.ctrl.Transformer2"}) {
            Class<?> reflClass = urlcl.loadClass(tranClass);
            Class<? extends ICtrlTransformer> transformerClass = reflClass.asSubclass(ICtrlTransformer.class);
            Constructor<? extends ICtrlTransformer> ctor = transformerClass.getConstructor(String.class);
            ICtrlTransformer transformerInstance = ctor.newInstance(tranClass);
            transformerInstance.init(tranClass,Integer.parseInt(tranClass.substring(tranClass.length()-1)));
            transformers.add(transformerInstance);
        }

        final AckedSource<String,BoxedUnit> ackedSource = ScalaHelper.IterableAckedSource(Arrays.asList("0_test tran","1_test tran","2_test tran"))
                .via(ScalaHelper.setupAckTracker(system.dispatcher()));
        final AckedSource<String,BoxedUnit> ackedPartialGraph =  ScalaHelper.addToGraph(ackedSource,ScalaHelper.convertJavaList(transformers),0,transformers.size(),system.dispatcher());

        final RunnableGraph<BoxedUnit> runnableGraph= ackedPartialGraph.to(AckedSink.ack());
        runnableGraph.run(mat);
    }


}