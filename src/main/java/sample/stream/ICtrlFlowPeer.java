package sample.stream;

import java.util.concurrent.Future;

/**
 * Created by andrewm on 10/13/2015.
 */
public interface ICtrlFlowPeer
{
    boolean onSyncMessage(byte[] message);
    void onAsyncMessage(byte[] message, Future<?> future);
}
