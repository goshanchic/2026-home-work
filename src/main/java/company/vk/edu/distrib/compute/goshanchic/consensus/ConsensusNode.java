package company.vk.edu.distrib.compute.goshanchic.consensus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsensusNode extends Thread {
    private static final long ELECTION_TIMEOUT_MS = 3000;
    private static final double FAILURE_PROBABILITY = 0.1;
    private static final long RECOVERY_TIME_MS = 5000;
    private static final Random RANDOM = new Random();

    private final int nodeId;
    private final Map<Integer, ConsensusNode> cluster;
    private final BlockingQueue<Message> inbox;
    private final AtomicInteger leaderId;
    private final AtomicBoolean running;
    private final AtomicBoolean alive;
    private volatile long lastPingResponse;
    private volatile boolean electionInProgress;
    private boolean randomFailuresEnabled = true;

    public ConsensusNode(int nodeId, Map<Integer, ConsensusNode> cluster) {
        this.nodeId = nodeId;
        this.cluster = cluster;
        this.inbox = new LinkedBlockingQueue<>();
        this.leaderId = new AtomicInteger(-1);
        this.running = new AtomicBoolean(true);
        this.alive = new AtomicBoolean(true);
        this.lastPingResponse = System.currentTimeMillis();
        this.electionInProgress = false;
    }

    public int getNodeId() { return nodeId; }
    public int getLeaderId() { return leaderId.get(); }
    public boolean isNodeAlive() { return alive.get(); }

    public void setAlive(boolean aliveValue) {
        this.alive.set(aliveValue);
        if (aliveValue) {
            this.lastPingResponse = System.currentTimeMillis();
        }
    }

    public void setRandomFailuresEnabled(boolean enabled) {
        this.randomFailuresEnabled = enabled;
    }

    public void sendMessage(Message message) {
        if (alive.get()) {
            inbox.offer(message);
        }
    }

    public void shutdown() {
        running.set(false);
        interrupt();
    }

    @Override
    public void run() {
        // Delay before first election to let all nodes start
        try { Thread.sleep(500 + nodeId * 100L); } catch (InterruptedException ignored) {}

        while (running.get()) {
            try {
                simulateFailure();
                if (!alive.get()) {
                    Thread.sleep(1000);
                    continue;
                }

                processInbox();
                checkLeader();
                sendPingToLeader();
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void simulateFailure() throws InterruptedException {
        if (randomFailuresEnabled && RANDOM.nextDouble() < FAILURE_PROBABILITY && alive.get()) {
            System.out.println("[Node " + nodeId + "] Simulating failure...");
            alive.set(false);
            new Thread(() -> {
                try {
                    Thread.sleep(RECOVERY_TIME_MS);
                    alive.set(true);
                    lastPingResponse = System.currentTimeMillis();
                    System.out.println("[Node " + nodeId + "] Recovered!");
                } catch (InterruptedException ignored) {}
            }).start();
        }
    }

    private void processInbox() {
        List<Message> messages = new ArrayList<>();
        inbox.drainTo(messages);
        for (Message msg : messages) {
            switch (msg.getType()) {
                case PING -> handlePing(msg);
                case ELECT -> handleElect(msg);
                case ANSWER -> handleAnswer(msg);
                case VICTORY -> handleVictory(msg);
            }
        }
    }

    private void handlePing(Message msg) {
        Message answer = new Message(MessageType.ANSWER, nodeId);
        cluster.get(msg.getSenderId()).sendMessage(answer);
    }

    private void handleElect(Message msg) {
        if (nodeId > msg.getSenderId()) {
            Message answer = new Message(MessageType.ANSWER, nodeId);
            cluster.get(msg.getSenderId()).sendMessage(answer);
            startElection();
        }
    }

    private void handleAnswer(Message msg) {
        electionInProgress = false;
    }

    private void handleVictory(Message msg) {
        leaderId.set(msg.getSenderId());
        electionInProgress = false;
        lastPingResponse = System.currentTimeMillis();
        System.out.println("[Node " + nodeId + "] New leader elected: " + msg.getSenderId());
    }

    private void checkLeader() {
        int currentLeader = leaderId.get();
        if (currentLeader == -1) {
            startElection();
            return;
        }
        if (currentLeader == nodeId) return;

        long timeSinceLastResponse = System.currentTimeMillis() - lastPingResponse;
        if (timeSinceLastResponse > ELECTION_TIMEOUT_MS) {
            System.out.println("[Node " + nodeId + "] Leader " + currentLeader + " is dead!");
            leaderId.set(-1);
            startElection();
        }
    }

    private void sendPingToLeader() {
        int currentLeader = leaderId.get();
        if (currentLeader != -1 && currentLeader != nodeId) {
            Message ping = new Message(MessageType.PING, nodeId);
            ConsensusNode leaderNode = cluster.get(currentLeader);
            if (leaderNode != null && leaderNode.isNodeAlive()) {
                leaderNode.sendMessage(ping);
            }
        }
    }

    private synchronized void startElection() {
        if (electionInProgress) return;
        electionInProgress = true;
        System.out.println("[Node " + nodeId + "] Starting election...");

        // Send ELECT to all higher nodes
        for (ConsensusNode node : cluster.values()) {
            if (node.getNodeId() > nodeId && node.isNodeAlive()) {
                node.sendMessage(new Message(MessageType.ELECT, nodeId));
            }
        }

        // Wait for answers
        try { Thread.sleep(2000); } catch (InterruptedException ignored) {}

        // If no answer from higher nodes, become leader
        if (electionInProgress) {
            leaderId.set(nodeId);
            System.out.println("[Node " + nodeId + "] I am the leader!");
            Message victory = new Message(MessageType.VICTORY, nodeId);
            for (ConsensusNode node : cluster.values()) {
                if (node.getNodeId() != nodeId && node.isNodeAlive()) {
                    node.sendMessage(victory);
                }
            }
        }
        electionInProgress = false;
    }
}