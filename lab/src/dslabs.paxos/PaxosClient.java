package dslabs.paxos;

import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.paxos.ClientTimer.CLIENT_RETRY_MILLIS;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
    private final Address[] servers;

    //TODO: declare fields for your implementation ...

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosClient(Address address, Address[] servers) {
        super(address);
        this.servers = servers;
        
        // TODO: initialize fields ...
    }

    @Override
    public synchronized void init() {
        // No need to initialize
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command operation) {
        // TODO: send command ...
    }

    @Override
    public synchronized boolean hasResult() {
        // TODO: check result available ...
        return false;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // TODO: get result ...
        return null;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        // TODO: handle paxos server reply ...
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // TODO: handle client request timeout ...
    }
}
