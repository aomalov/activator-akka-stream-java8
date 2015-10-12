package sample.stream;

import akka.actor.ActorSystem;
import akka.dispatch.Foreach;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.*;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.util.Arrays;
import scala.concurrent.Future;

/**
 * Created by andrewm on 10/11/2015.
 */
public class MaterializerTest {
    public static void main(String[] args) throws IOException {
        final ActorSystem system = ActorSystem.create("Sys");
        final ActorMaterializer materializer = ActorMaterializer.create(system);

        Source<Integer, BoxedUnit> src = Source.from(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

//        Flow<Integer, Integer, BoxedUnit> stage1 = Flow.of(Integer.class).map(i -> -i);
//
//        src.viaMat(stage1,Keep.both()).to(Sink.foreach(i -> System.out.println(i))).run(materializer);

        final Sink<Integer, Future<Integer>> sumSink =
                Sink.<Integer, Integer>fold(0, (acc, elem) -> acc + elem);

        final RunnableGraph<Future<Integer>> counter =
                src.map(t -> 1).toMat(sumSink, Keep.right());

        final Future<Integer> sum = counter.run(materializer);

        sum.foreach(new Foreach<Integer>() {
            public void each(Integer c) {
                System.out.println("Total tweets processed: " + c);
            }
        }, system.dispatcher());

    }
}
