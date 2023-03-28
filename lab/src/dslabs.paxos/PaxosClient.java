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
    private int sequenceNumber;
    private PaxosRequest request;
    private PaxosReply reply;
    private ClientTimer clientTimer;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosClient(Address address, Address[] servers) {
        super(address);
        this.servers = servers;
        
        // TODO: initialize fields ...
        sequenceNumber = 0;
        clientTimer = new ClientTimer();
    }

    @Override
    public synchronized void init() {
        // No need to initialize
        set(clientTimer,CLIENT_RETRY_MILLIS);
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command operation) {
        // TODO: send command ...
        sequenceNumber++;
        request = new PaxosRequest(this.address(), sequenceNumber, operation);
        reply = null;
        for (Address server : servers) {
            send(request, server);
        }
    }

    @Override
    public synchronized boolean hasResult() {
        // TODO: check result available ...
        return reply != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {
        // TODO: get result ...
        while (reply == null) {
            wait();
        }
        return reply.result();
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
        // TODO: handle paxos server reply ...
        if (reply == null && m.sequenceNum() == sequenceNumber) {
            request = null;
            reply = m;
            notifyAll();
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        // TODO: handle client request timeout ...
        if (request != null && !hasResult()) {
            for (Address server : servers) {
                send(request, server);
            }
        }
        set(t, CLIENT_RETRY_MILLIS);
    }
}
