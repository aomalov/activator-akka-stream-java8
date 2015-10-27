package sample.stream;

import scala.runtime.BoxedUnit;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Created by andrewm on 10/13/2015.
 */
public interface ICtrlFlowPeer
{
    void onNext(byte[] message,CompletableFuture<Void> cfPromise);
    //void onAsyncMessage(byte[] message, Future<?> future);
}
