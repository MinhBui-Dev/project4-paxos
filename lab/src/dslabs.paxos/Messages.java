package dslabs.paxos;

// We have added 9 messages and 5 extra timers.
// TODO: add messages for PAXOS here ...

import dslabs.framework.Message;
import java.util.Set;
import java.util.TreeMap;
import lombok.Data;

@Data
class P1request implements Message {
    private final Ballot ballotNum;
}

@Data
class P1reply implements Message {
    private final Ballot ballotNum;
    private final Set<Pvalue> accepted;

    private final TreeMap<Integer, PaxosRequest> decisionsLog;
}

@Data
class P2request implements Message {
    private final Ballot ballotNum;

    private final int slotNum;
    private final PaxosRequest paxosRequest;

}

@Data
class P2CatchupRequest implements Message {
    private final Ballot ballotNum;
    private final TreeMap<Integer, PaxosRequest> requestLog;
}




@Data
class P2reply implements Message {
    private final Ballot ballotNum;
    private final int slotNum;
    private final PaxosRequest paxosRequest;
}

@Data
class Decision implements Message {
    private final int slotNum;
    private final PaxosRequest paxosRequest;
}
@Data
class Heartbeat implements Message {
    private final Ballot ballotNum;
    private final int slot_GC;
    private final TreeMap<Integer, PaxosRequest> decisionsLog;
    // TODO: add fields for heartbeat message
}

@Data
class HeartbeatReply implements Message {
    private final Ballot ballotNum;
    private final int slotExecuted;
}

