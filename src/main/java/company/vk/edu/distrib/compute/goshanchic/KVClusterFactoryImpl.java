package company.vk.edu.distrib.compute.goshanchic;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KVClusterFactoryImpl extends company.vk.edu.distrib.compute.KVClusterFactory {
    private final int replicationFactor;
    private final int defaultAck;

    public KVClusterFactoryImpl() {
        super();
        this.replicationFactor = 1;
        this.defaultAck = 1;
    }

    public KVClusterFactoryImpl(int replicationFactor, int defaultAck) {
        super();
        this.replicationFactor = replicationFactor;
        this.defaultAck = defaultAck;
    }

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new ClusterImpl(ports);
    }

    private class ClusterImpl implements KVCluster {
        private final List<Integer> ports;
        private final Map<Integer, KVService> services = new ConcurrentHashMap<>();

        ClusterImpl(List<Integer> ports) {
            this.ports = ports;
        }

        @Override
        public void start() {
            for (int port : ports) {
                startService(port);
            }
        }

        @Override
        public void start(String endpoint) {
            int port = parsePort(endpoint);
            if (!services.containsKey(port)) {
                startService(port);
            }
        }

        @Override
        public void stop() {
            for (KVService service : services.values()) {
                service.stop();
            }
            services.clear();
        }

        @Override
        public void stop(String endpoint) {
            KVService service = services.remove(parsePort(endpoint));
            if (service != null) {
                service.stop();
            }
        }

        @Override
        public List<String> getEndpoints() {
            return services.keySet().stream()
                    .map(p -> "http://localhost:" + p)
                    .toList();
        }

        private void startService(int port) {
            try {
                InMemoryDao dao = new InMemoryDao();
                KVService service = new KVServiceImpl(port, ports, dao, replicationFactor, defaultAck);
                service.start();
                services.put(port, service);
            } catch (IOException e) {
                throw new RuntimeException("Failed to start service on port " + port, e);
            }
        }

        private int parsePort(String endpoint) {
            String hostPort = endpoint.replace("http://", "").replace("https://", "");
            int colonIdx = hostPort.indexOf(':');
            if (colonIdx >= 0) {
                return Integer.parseInt(hostPort.substring(colonIdx + 1));
            }
            return Integer.parseInt(hostPort);
        }
    }
}
