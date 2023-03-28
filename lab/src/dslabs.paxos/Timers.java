package dslabs.paxos;

import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;

    // TODO: add fields for client timer ...
    private int sequenceNumber;
}

// We have added 9 messages and 5 extra timers.
// TODO: add more timers here ...

@Data
final class HeartbeatTimer implements Timer {
    static final int HEARTBEAT_MILLIS = 200;
    private final Ballot ballotNumber;
}

@Data
final class ElectionTimer implements Timer {
    static final int ELECTION_MILLIS = 100;
    private final Ballot ballotNumber;
}

@Data
final class ProposalTimer implements Timer {
    static final int PROPOSAL_MILLIS = 50;
    private final Ballot ballotNumber;
    private final int slotNumber;
}

@Data
final class ServerTimer implements Timer {
    static final int SERVER_RETRY_MILLIS = 50;
    private final Ballot ballotNumber;
}