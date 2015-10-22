package sample.stream;

import scala.Unit;
import scala.compat.java8.FutureConverters;
import scala.concurrent.Promise;

import java.util.concurrent.CompletableFuture;

/**
 * Created by andrewm on 10/20/2015.
 */
public class AckedFlowMsg<T> {
    public final CompletableFuture<T> ackPromise;
    public final T msgData;

    public AckedFlowMsg(T msgData) {
        this.msgData = msgData;
        this.ackPromise = new CompletableFuture<>();
        this.ackPromise.handle((ok, ex) -> {
            if (ok != null) {
                System.out.println("ACKED from inside - " + ok);
                return 1;
            } else {
                System.out.println("Problem -" + ex);
                return 0;
            }
        });
    }
}
