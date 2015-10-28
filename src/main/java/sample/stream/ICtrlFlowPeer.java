package sample.stream;

import scala.runtime.BoxedUnit;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Created by andrewm on 10/13/2015.
 */
public interface ICtrlFlowPeer<T>
{
    void onNext(T message,CompletableFuture<Void> cfPromise);
    //void onAsyncMessage(byte[] message, Future<?> future);
}

abstract class SyncAppReceiver<T> implements ICtrlFlowPeer<T> {

    @Override
    public void onNext(T msg, CompletableFuture<Void> cf) {
        try {
            receive(msg);
            System.out.println("CF completed from the sync stub");
            cf.complete(null);
        } catch (Throwable t) {
            cf.completeExceptionally(t);
        }

    }

    abstract void receive(T msg) throws Exception;
}
