package sample.stream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import akka.stream.*;
import akka.stream.io.SynchronousFileSink;
import akka.stream.io.SynchronousFileSource;
import akka.util.ByteString;
import scala.Function1;
import scala.concurrent.Future;
import scala.concurrent.forkjoin.ThreadLocalRandom;
import scala.runtime.BoxedUnit;
import akka.actor.ActorSystem;
import akka.dispatch.OnComplete;
import scala.concurrent.Future;
import akka.japi.function.Procedure2;
import akka.stream.javadsl.*;
import scala.util.Try;

import javax.swing.*;

public class WritePrimes {
  public static void main(String[] args) throws IOException {
    final ActorSystem system = ActorSystem.create("Sys");
    final ActorMaterializer materializer = ActorMaterializer.create(system);

      ArrayList<Integer> lst=new ArrayList<>(1000);
      for(int k=0;k<1000;k++) lst.add(Integer.valueOf(k));
    // generate random numbers
    final int maxRandomNumberSize = 1000000;
    final Source<Integer, BoxedUnit> primeSource = Source.from(new RandomIterable(maxRandomNumberSize,10000)).
        // filter prime numbers
        filter(WritePrimes::isPrime).
        // and neighbor +2 is also prime
        filter(prime -> isPrime(prime + 2));

    //final Source<Integer, BoxedUnit> intSource = Source.from(new RandomIterable(maxRandomNumberSize,1000));
      //final Source<Integer, BoxedUnit> intSource = Source.from(new StraightIterable(1000));
      final Source<Integer, BoxedUnit> intSource = Source.from(lst);

    // write to file sink
      final PrintWriter writeToFile = new PrintWriter(new FileOutputStream("target/primes.txt"), true);
      Sink<Integer, Future<BoxedUnit>> fileSink = Sink.foreach(i -> writeToFile.println(i));


      // console output sink
    Sink<Integer, Future<BoxedUnit>> consoleSink = Sink.foreach(i -> System.out.println(String.format("Quick sink : %s",i)));

    Sink<Integer, Future<BoxedUnit>> consoleSink2 = Sink.foreach(i -> System.out.println(String.format("=================Slow sink : %s",i)));

      FlowGraph.factory().closed(builder -> {
          //final Outlet<Integer> intSrc = builder.source(Source.from(new RandomIterable(maxRandomNumberSize,10000)));

          //final UniformFanOutShape<Integer, Integer> fanoutBroadcast = builder.graph(Broadcast.create(2));
          final UniformFanOutShape<Integer, Integer> fanoutBalance = builder.graph(Balance.create(2));

          //final UniformFanInShape<Integer, Integer> faninMerger = builder.graph(Merge.create(2));

          final FlowShape<Integer, Integer> primeFilter = builder.graph(Flow.of(Integer.class).filter(i -> isPrime(i)).filter(i -> isPrime(i+2)));
          final FlowShape<Integer, Integer> transfNeg = builder.graph(Flow.of(Integer.class).map(i -> -i));

          //final Inlet<Integer> G = builder.sink(Sink.foreach(System.out::println));

          builder.from(intSource).via(fanoutBalance).via(primeFilter).via(transfNeg).to(consoleSink2);   //to(consoleSink2);
          builder.from(fanoutBalance).to(consoleSink);
      }).run(materializer);

      writeToFile.close();

      //TODO:  create consume code - for completing the flow - to shut down everything

      //TODO: try to fold to summ up both output sinks - how many values arrived to each sink

    // connect the graph, materialize and retrieve the completion Future
/*    final Future<Long> future = FlowGraph.factory().closed(slowSink, (b, sink) -> {
      final UniformFanOutShape<Integer, Integer> bcast = b.graph(Broadcast.<Integer> create(2));
      b.from(primeSource).via(bcast).to(sink)
                        .from(bcast).to(consoleSink);
    }).run(materializer);

    future.onComplete(new OnComplete<Long>() {
      @Override
      public void onComplete(Throwable failure, Long success) throws Exception {
        if (failure != null) System.err.println("Failure: " + failure);
        system.shutdown();
      }
    }, system.dispatcher());*/
  }

  private static boolean isPrime(int n) {
      //System.out.println("testing : "+n);
    if (n <= 1)
      return false;
    else if (n == 2)
      return true;
    else {
      for (int i = 2; i < n; i++) {
        if (n % i == 0)
          return false;
      }
      return true;
    }
  }
}

class RandomIterable implements Iterable<Integer> {

  private final int maxRandomNumberSize;
  private int maxCount;

  RandomIterable(int maxRandomNumberSize,int maxCount) {
    this.maxRandomNumberSize = maxRandomNumberSize;
    this.maxCount=maxCount;
  }

  @Override
  public Iterator<Integer> iterator() {
    return new Iterator<Integer>() {
      @Override
      public boolean hasNext() {
        return (maxCount-- >0);
        //return true;
      }

      @Override
      public Integer next() {
        return ThreadLocalRandom.current().nextInt(maxRandomNumberSize);
      }
    };
  }
}
