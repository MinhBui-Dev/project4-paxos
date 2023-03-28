package dslabs.paxos;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.kvstore.KVStore;
import dslabs.kvstore.KVStore.Get;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static dslabs.paxos.ElectionTimer.ELECTION_MILLIS;
import static dslabs.paxos.HeartbeatTimer.HEARTBEAT_MILLIS;
import static dslabs.paxos.ProposalTimer.PROPOSAL_MILLIS;
import static dslabs.paxos.ServerTimer.SERVER_RETRY_MILLIS;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
    static final int INITIAL_BALLOT_NUMBER = 0;
    private final Address[] servers;

    // TODO: declare fields for your implementation ...
    private final AMOApplication<KVStore> amoApplication;

    // Acceptor State
    private final ArrayList<Pvalue> accepted = new ArrayList<>();

    // Replica State
    private int slotIn = 1, slotOut = 1, slotGC = 0, slotChosen = 0;
    private final HashMap<Integer, PaxosRequest> decisions = new HashMap<>();
    private Address leader = null;
    private final Address self;
    // Proposer State
    private Ballot myBallot = null;
    private final HashMap<Integer, PaxosRequest> proposals = new HashMap<>();
    private final Set<Address> waitingVote = new HashSet<>();
    private final HashMap<Integer,Set<Address>> waitingAccept = new HashMap<>(); //May need multimap to keep track of # of p2b received
    private final HashMap<Integer,Set<Address>> P2BReceived = new HashMap<>();
    private boolean ballotchange = false, active = false;
    private final Set<PaxosRequest> pending = new HashSet<>();
    private final ArrayList<PaxosRequest> getPending = new ArrayList<>();
    private final TreeMap<Integer,Set<Address>> executedSlots = new TreeMap<>();
    private int[] count = {0,0};

    //private Multimap<Integer, Integer> map = ArrayListMultimap.create();


    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;
        this.self = address;

        // wrap app inside AMOApplication ...
        this.amoApplication = new AMOApplication<>((KVStore) app);
    }


    @Override
    public void init() {
        // TODO: initialize fields ...
        // TODO: send ballot to vote distinguished leader ...
        // System.out.println("Initializing Paxos Server: " + self.toString());


        if (servers.length == 1) {
            active = true;
        } else {
            this.myBallot = new Ballot(INITIAL_BALLOT_NUMBER, this.self);
            for (Address server : servers) {
                if (!self.equals(server)) {
                    // System.out.println("Sending ballot to: " + server.toString() + " from: " + self.toString());
                    send(new P1request(myBallot), server);
                    waitingVote.add(server);
                }
            }
            // System.out.println("Done sending ballots from: " + self.toString() + ". Starting election timer");

            // TODO: start election timer ...
            set(new ServerTimer(myBallot), SERVER_RETRY_MILLIS);
        }
    }


    /* -------------------------------------------------------------------------
        Interface Methods

        Be sure to implement the following methods correctly. The test code uses
        them to check correctness more efficiently.
       -----------------------------------------------------------------------*/

    /**
     * Return the status of a given slot in the servers's local log.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's status
     */
    public PaxosLogSlotStatus status(int logSlotNum) {
        // Your code here...
        if (logSlotNum <= slotGC) {
            return PaxosLogSlotStatus.CLEARED;
        }
        else {
            if (decisions.containsKey(logSlotNum)) {
                return PaxosLogSlotStatus.CHOSEN;
            } else if (proposals.containsKey(logSlotNum)) {
                return PaxosLogSlotStatus.ACCEPTED;
            } else {
                return PaxosLogSlotStatus.EMPTY;
            }
        }
    }

    /**
     * Return the command associated with a given slot in the server's local
     * log. If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
     * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}. If
     * clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this
     * method should unwrap them before returning.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's contents or {@code null}
     */
    public Command command(int logSlotNum) {
        // Your code here...
        if (decisions.containsKey(logSlotNum)) {
            return decisions.get(logSlotNum).command();
        } else if (proposals.containsKey(logSlotNum)) {
            return proposals.get(logSlotNum).command();
        } else {
            return null;
        }
    }

    /**
     * Return the index of the first non-cleared slot in the server's local
     * log.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     */
    public int firstNonCleared() {
        // Your code here...
        return slotGC +1;
    }

    /**
     * Return the index of the last non-empty slot in the server's local log. If
     * there are no non-empty slots in the log, this method should return 0.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     */
    public int lastNonEmpty() {
        // Your code here...
        return  slotIn-1;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private void handlePaxosRequest(PaxosRequest m, Address sender) {
        // handle paxos request ...
        //System.out.println("Received Paxos Request from: " + sender.toString() + " to: " + self.toString());
        if (active) {
            AMOCommand command = new AMOCommand(m.command(),m.sequenceNum(),m.id());
            if (amoApplication.alreadyExecuted(command)) {
                AMOResult result = amoApplication.execute(command);
                send(new PaxosReply(sender, m.sequenceNum(), result.result()),
                        sender);
            } else if (proposals.containsValue(m) ) {
                return;
            } else {
                // Add request to log
                if (servers.length == 1) {
                    proposals.put(slotIn, m);
                    decisions.put(slotIn, m);
                    slotIn++;
                    perform(m);
                } else {
                    pending.add(m);
                    Iterator<PaxosRequest> it = pending.iterator();
                    while (it.hasNext()) {
                        PaxosRequest request = it.next();
                        propose(request);
                        it.remove();
                    }
                }
            }
        } else {
            if (leader == null) {
                pending.add(m);
            }
        }
    }

    private void handleP1request(P1request m, Address sender) {
        //System.out.println("Received P1request from: " + sender.toString() + " to: " + self.toString() + " with ballot: " + m.ballotNum().toString());
        /*if (myBallot.compareTo(m.ballotNum()) < 0) {
            myBallot = m.ballotNum();
            ballotchange = true;
            // Co-location with leader -> preempted leader
            active = false;
            leader = m.ballotNum().address();
            count[1] = 0;
            count[0] = 1;
        }*/
        preempted(m.ballotNum());
        send(new P1reply(myBallot, new HashSet<Pvalue>(accepted), new TreeMap<>(decisions)), sender);
    }
    private void handleP1reply(P1reply m, Address sender) {
        // handle leader election reply ...
        //System.out.println("Received P1reply from: " + sender.toString() + " to: " + self.toString() + " with ballot: " + m.ballotNum().toString());
        if (!active && !ballotchange && myBallot.compareTo(m.ballotNum()) == 0 && waitingVote.contains(sender)) {
            waitingVote.remove(sender);

            // Compare decisions log
            syncLog(m.decisionsLog());

            // Add all accepted values to accepted set
            accepted.addAll(m.accepted());
            if (2 * waitingVote.size() < servers.length) { // Adopted vote
                if (accepted.size() > 0) {
                    Map<Integer,Ballot> max = new HashMap<>();
                    //System.out.println("Size of accepted: " + accepted.size() + " from: " + self.toString());

                    for (Pvalue pv: accepted) {
                        if (pv.slot_num() <= slotGC) {
                            continue;
                        }
                        Ballot bn = max.get(pv.slot_num());
                        if (bn == null || bn.compareTo(pv.ballot()) < 0) {
                            max.put(pv.slot_num(), pv.ballot());
                            proposals.put(pv.slot_num(),pv.paxosRequest());
                        }
                    }
                    //TreeMap<Integer,PaxosRequest> batchRequest = new TreeMap<>();
                    for (int sn: proposals.keySet()) {
                        // TODO: check if slot number is higher than slotOut
                        if (sn < slotOut) {
                            continue;
                        }
                        if (sn >= slotIn) {
                            slotIn = sn + 1;
                        }
                        //batchRequest.put(sn, proposals.get(sn));
                        P2request p2request = new P2request(myBallot, sn, proposals.get(sn));
                        commander(p2request);
                    }
                    /*if (batchRequest.size() > 0) {
                        P2request p2req = new P2request(myBallot, batchRequest);
                        commander(p2req);
                    }*/
                }
                waitingVote.clear();
                active = true;
                /*for (Address a : servers) {
                    if (!a.equals(self)) {
                        send(new Heartbeat(myBallot, slotGC,
                                new TreeMap<>(decisions)), a);
                    }
                }*/
            }
        }
    }



    private void handleP2request(P2request m, Address sender) {
        if (myBallot == null || (myBallot.compareTo(m.ballotNum()) <= 0 && m.slotNum() >= slotOut)) {
            /*if (myBallot.compareTo(m.ballotNum()) < 0) {
                myBallot = m.ballotNum();
                ballotchange = true;
                // Co-location with leader -> preempted leader
                active = false;
                leader = m.ballotNum().address();
                count[1] = 0;
                count[0] = 1;
            }*/
            preempted(m.ballotNum());

            //TreeMap<Integer,PaxosRequest> batchRequest = m.requestLog();


            for(Pvalue pv: accepted) {
                if (pv.slot_num() == m.slotNum()) {
                    accepted.remove(pv);
                    break;
                }
            }
            accepted.add(new Pvalue(m.slotNum(), m.ballotNum(),m.paxosRequest()));
            proposals.put(m.slotNum(),m.paxosRequest());
            if (m.slotNum() >= slotIn) {
                slotIn = m.slotNum() + 1;
            }

            send(new P2reply(myBallot, m.slotNum(),m.paxosRequest()), sender);
        } else {
            send(new P2reply(myBallot, m.slotNum(),null), sender);
        }
    }

    private void handleP2reply(P2reply m, Address sender) {
        if (myBallot.equals(m.ballotNum()) && active && P2BReceived.containsKey(m.slotNum()) && m.paxosRequest() != null) {

            Set<Address> set = P2BReceived.get(m.slotNum());

            if (!set.contains(sender)){
                return;
            } else {
                set.remove(sender);
            }


            /*if (P2BReceived.containsKey(m.slotNum())) {
                Set<Address> set = P2BReceived.get(m.slotNum());
                if (set.contains(sender)){
                    return;
                }
                set.add(sender);
            } else {
                Set<Address> set = new HashSet<>();
                set.add(sender);
                P2BReceived.put(m.slotNum(), set);
            }*/


            if (2 * set.size() < servers.length) { // Quorum reached, send decision
                //proposals.put(m.slotNum(), m.paxosRequest());
                P2BReceived.remove(m.slotNum());
                if (decisions.containsKey(m.slotNum())) {
                    return;
                }
                accepted.add(new Pvalue(m.slotNum(), m.ballotNum(),m.paxosRequest()));
                decisions.put(m.slotNum(), m.paxosRequest());
                for (;;){
                    PaxosRequest paxosRequest = decisions.get(slotOut);
                    if (paxosRequest == null) {
                        break;
                    }
                    PaxosRequest paxosRequest1 = proposals.get(slotOut);
                    // TODO: Maybe not be applicable
                    if (paxosRequest1 != null && !paxosRequest1.equals(paxosRequest)) {
                        //System.out.print("Request not matched, reproposing");
                        propose(paxosRequest1);
                    }
                    perform(paxosRequest);
                }
                /*for (Address server: servers) {
                    if (!server.equals(self)) {
                        send(new Decision(m.slotNum(), m.paxosRequest()), server);
                    }
                }*/
            }

        } else {
            //Handle Preempted State from leader election
            preempted(m.ballotNum());
        }
    }

    private void handleDecision(Decision m, Address sender) {
        // TODO: handle decision ...
        if (decisions.containsKey(m.slotNum()) || m.slotNum() <= slotGC) {
            return;
        }
        if (m.slotNum() >= slotIn) {
            slotIn = m.slotNum() + 1;
        }
        decisions.put(m.slotNum(), m.paxosRequest());

        for (; ; ) {
            PaxosRequest paxosRequest = decisions.get(slotOut);
            if (paxosRequest == null) {
                break;
            }
            perform(paxosRequest);
        }
    }

    private void handleHeartbeat(Heartbeat m, Address sender) {
        if (myBallot.compareTo(m.ballotNum()) <= 0) {
            if (myBallot.compareTo(m.ballotNum()) < 0) {
                myBallot = m.ballotNum();
                ballotchange = true;
                // Co-location with leader -> preempted leader
                active = false;
                leader = m.ballotNum().address();
            }
            // Perform garbage collection
            if ((m.slot_GC() > slotGC)) {

                for (int i = slotGC; i <= m.slot_GC(); i++) {
                    garbageCollection(i);
                }
                slotGC = m.slot_GC();
            }
            // compare decisions
            syncLog(m.decisionsLog());
            send(new HeartbeatReply(myBallot, slotOut-1), sender);
            count[0] = 1;

        }
    }

    private void handleHeartbeatReply(HeartbeatReply m, Address sender) {
        if (myBallot.equals(m.ballotNum())) {
            if (m.slotExecuted() == 0) {
                return;
            }
            if (executedSlots.containsKey(m.slotExecuted())) {
                Set<Address> servers = executedSlots.get(m.slotExecuted());
                servers.add(sender);
            } else {
                Set<Address> servers = new HashSet<>();
                servers.add(sender);
                executedSlots.put(m.slotExecuted(), servers);
            }

            int slotToGC = slotGC;

            for (Integer sn: executedSlots.keySet()) {
                if (executedSlots.get(sn).size() == servers.length-1) {
                    slotToGC = sn;
                }
            }

            if (slotToGC > slotGC) {
                for (int i = slotGC; i < slotToGC; i++) {
                    garbageCollection(i);
                }
                slotGC = slotToGC - 1;
            }
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onServerTimer(ServerTimer t) {
        if (!active && !ballotchange) {
            for (Address s : waitingVote) {
                send(new P1request(myBallot), s);
            }
        } else if (active) {
            // Resend Request
            for (int sn: P2BReceived.keySet()) {
                Set<Address> set = P2BReceived.get(sn);
                for (Address server: set) {
                    send(new P2request(myBallot, sn, proposals.get(sn)), server);
                }
            }
            // Resend heartbeat
            for (Address a: servers) {
                if (!a.equals(self)) {
                    send(new Heartbeat(myBallot, slotGC, new TreeMap<>(decisions)), a);

                }
            }
            //set(t, HEARTBEAT_MILLIS);
        } else {
            // Check if leader is alive
            int sum = count[0] + count[1];
            if (sum == 0) {
                startElection(myBallot);
            } else {
                count[1] = count[0];
                count[0] = 0;
            }


        }
        set(t, SERVER_RETRY_MILLIS);

    }
    private void onProposalTimer(ProposalTimer t) {
        if (myBallot.equals(t.ballotNumber()) && (!decisions.containsKey(t.slotNumber())) && t.slotNumber() > slotGC){
            for (Address server: servers) {
                if (!server.equals(self)) {
                    send(new P2request(myBallot, t.slotNumber(), proposals.get(t.slotNumber())), server);
                    set(t, PROPOSAL_MILLIS);
                }
            }
        }
    }
    private void onElectionTimer(ElectionTimer t) {
        // Check if majority is reached on leader election or preempted
        if (myBallot.equals(t.ballotNumber()) && !active){
            for (Address s: waitingVote) {
                send(new P1request(myBallot), s);
            }
            set(t, ELECTION_MILLIS);
        }
    }

    private void onHeartbeatTimer(HeartbeatTimer t) {
        // TODO: handle heartbeat timer ...
        if (active && myBallot.equals(t.ballotNumber())) {
            for (Address a: servers) {
                if (!a.equals(self)) {
                    send(new Heartbeat(myBallot, slotGC, new TreeMap<>(decisions)), a);

                }
            }
            set(t, HEARTBEAT_MILLIS);
        }
    }

    private void garbageCollection(int i) {
        decisions.remove(i);
        proposals.remove(i);
        waitingAccept.remove(i);
        P2BReceived.remove(i);
        executedSlots.remove(i);

        Iterator<Pvalue> iterator = accepted.iterator();
        while (iterator.hasNext()) {
            Pvalue pv = iterator.next();
            if (pv.slot_num() == i) {
                iterator.remove();
            }
        }
    }


    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    private void commander(P2request p2req) {
        Set<Address> serverSet = new HashSet<>();
        for (Address s: servers) {
            if (!s.equals(self)) {
                send(p2req, s);
                serverSet.add(s);
            }
        }
        P2BReceived.put(p2req.slotNum(), serverSet);
    }

    private void preempted(Ballot bn) {
        if (myBallot.compareTo(bn) < 0) {
            myBallot = bn;
            leader = myBallot.address();
            active = false;
            ballotchange = true;
            waitingVote.clear();
            P2BReceived.clear();
            count[0] = 1;
        }

    }

    private void propose(PaxosRequest m) {
        if (!decisions.containsKey(slotIn)) {
            proposals.put(slotIn, m);
            commander(new P2request(myBallot, slotIn, m));
        }
        slotIn++;
    }

    private void perform(PaxosRequest m) {
        for (int sn = 1; sn < slotOut; sn++){
            if (m.equals(decisions.get(sn))) {
                slotOut++;
                return;
            }
        }
        AMOCommand amoCommand = new AMOCommand(m.command(), m.sequenceNum(), m.id());
        AMOResult amoResult = amoApplication.execute(amoCommand);
        if (active) {
            send(new PaxosReply(m.id(), m.sequenceNum(), amoResult.result()), m.id());
        }
        slotOut++;
    }

    private void syncLog(
            TreeMap<Integer, PaxosRequest> messageLog) {
        if (decisions.size() < messageLog.size()) {
            Set<Integer> keys = messageLog.keySet();
            Collections.sort(new ArrayList<>(keys));

            for (Integer sn : keys) {
                if (!decisions.containsKey(sn) && sn > slotGC) {
                    if (sn >= slotIn) {
                        slotIn = sn + 1;
                    }
                    decisions.put(sn, messageLog.get(sn));
                }
            }

            for (; ; ) {
                PaxosRequest paxosRequest = decisions.get(slotOut);
                if (paxosRequest == null) {
                    break;
                }
                perform(paxosRequest);
            }
        }
    }

    private void startElection(Ballot bn) {
        this.myBallot = new Ballot(bn.sequenceNum() + 1, self);
        this.waitingVote.clear();
        leader = null;
        ballotchange = false;
        for (Address server : servers) {
            if (!self.equals(server)) {
                send(new P1request(myBallot), server);
                waitingVote.add(server);
            }
        }
    }
}