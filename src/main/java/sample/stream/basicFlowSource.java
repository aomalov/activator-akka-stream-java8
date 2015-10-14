package sample.stream;

import akka.dispatch.Futures;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.concurrent.Future;

/**
 * Created by andrewm on 10/13/2015.
 */



public class basicFlowSource implements ICtrlFlowPeer {

    private final scala.concurrent.Future<String> futureAdapter;

    public basicFlowSource() {
        futureAdapter = Futures.<String>successful("OK");
    }

    @Override
    public boolean onSyncMessage(byte[] message) {
        return false;
    }

    @Override
    public void onAsyncMessage(byte[] message, Future<?> future)  {
        throw new NotImplementedException();
    }

    public scala.concurrent.Future<String> getFutureAdapter() {
        return futureAdapter;
    }


}
