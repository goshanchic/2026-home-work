package company.vk.edu.distrib.compute.goshanchic.consensus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LeaderElectionDemo {

    public static void main(String[] args) throws InterruptedException {
        int nodeCount = 5;
        Map<Integer, ConsensusNode> cluster = new ConcurrentHashMap<>();

        System.out.println("=== Starting cluster with " + nodeCount + " nodes ===");
        for (int i = 1; i <= nodeCount; i++) {
            ConsensusNode node = new ConsensusNode(i, cluster);
            cluster.put(i, node);
        }

        for (ConsensusNode node : cluster.values()) {
            node.start();
        }

        // Работаем 30 секунд
        Thread.sleep(30000);

        System.out.println("\n=== Final State ===");
        for (ConsensusNode node : cluster.values()) {
            System.out.printf("Node %d | Alive: %b | Leader: %d%n",
                    node.getNodeId(), node.isNodeAlive(), node.getLeaderId());
        }

        for (ConsensusNode node : cluster.values()) {
            node.shutdown();
        }
    }
}