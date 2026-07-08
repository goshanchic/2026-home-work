package company.vk.edu.distrib.compute.goshanchic;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.goshanchic.grpc.KVInternalServiceGrpc;
import company.vk.edu.distrib.compute.goshanchic.grpc.KVInternal.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"PMD.GodClass", "PMD.ExcessiveImports", "PMD.CouplingBetweenObjects"})
public class KVServiceImpl implements ReplicatedService, AuditableKVService {
    private static final Logger LOG = LoggerFactory.getLogger(KVServiceImpl.class);
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final String PARAM_ID = "id";
    private static final String PARAM_ACK = "ack";

    private static final int STATUS_OK = 200;
    private static final int STATUS_CREATED = 201;
    private static final int STATUS_ACCEPTED = 202;
    private static final int STATUS_BAD_REQUEST = 400;
    private static final int STATUS_NOT_FOUND = 404;
    private static final int STATUS_METHOD_NOT_ALLOWED = 405;
    private static final int STATUS_INTERNAL_ERROR = 500;
    private static final int STATUS_SERVICE_UNAVAILABLE = 503;

    private final int port;
    private final HttpServer httpServer;
    private final GoshanchicGrpcServer grpcServer;
    private final InMemoryDao dao;
    private final List<String> clusterNodes;
    private final String selfAddress;
    private final Map<String, ManagedChannel> grpcChannels = new ConcurrentHashMap<>();
    private final Set<Integer> disabledReplicas = new CopyOnWriteArraySet<>();

    private final int replicationFactor;
    private int defaultAck;
    private boolean asyncMode = true;
    private final AtomicReference<KafkaProducer<String, String>> kafkaProducer = new AtomicReference<>();
    private final ReentrantLock producerLock = new ReentrantLock();
    private String bootstrapServers = "localhost:9092";

    public KVServiceImpl(int port, List<Integer> allPorts, InMemoryDao dao) throws IOException {
        this(port, allPorts, dao, 1, 1);
    }

    public KVServiceImpl(int port, List<Integer> allPorts, InMemoryDao dao,
                         int replicationFactor, int defaultAck) throws IOException {
        if (defaultAck > replicationFactor) {
            throw new IllegalArgumentException(
                    "ack (" + defaultAck + ") cannot exceed replicationFactor (" + replicationFactor + ")");
        }

        this.port = port;
        this.dao = dao;
        this.selfAddress = "http://localhost:" + port;
        int grpcPort = port + 1000;
        this.clusterNodes = allPorts.stream()
                .map(p -> "http://localhost:" + p + "?grpcPort=" + (p + 1000))
                .collect(Collectors.toList());
        this.httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        this.grpcServer = new GoshanchicGrpcServer(grpcPort, dao);
        this.replicationFactor = Math.min(replicationFactor, clusterNodes.size());
        this.defaultAck = defaultAck;

        setupEndpoints();
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return replicationFactor;
    }

    @Override
    public void disableReplica(int nodeId) {
        disabledReplicas.add(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        disabledReplicas.remove(nodeId);
    }

    @Override
    public void setAsync(boolean enabled) {
        this.asyncMode = enabled;
    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        KafkaProducer<String, String> old = kafkaProducer.getAndSet(null);
        if (old != null) {
            old.close();
        }
    }

    private KafkaProducer<String, String> ensureProducer() {
        KafkaProducer<String, String> p = kafkaProducer.get();
        if (p == null) {
            producerLock.lock();
            try {
                p = kafkaProducer.get();
                if (p == null) {
                    p = createProducer(bootstrapServers);
                    kafkaProducer.set(p);
                }
            } finally {
                producerLock.unlock();
            }
        }
        return p;
    }

    private static KafkaProducer<String, String> createProducer(String servers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "100");
        return new KafkaProducer<>(props);
    }

    private void setupEndpoints() {
        httpServer.createContext("/v0/status", exchange -> {
            try {
                sendResponse(exchange, STATUS_OK, "OK".getBytes());
            } catch (IOException e) {
                exchange.close();
            }
        });

        httpServer.createContext("/v0/entity", exchange -> {
            try {
                processEntityRequest(exchange);
            } catch (Exception e) {
                try {
                    sendResponse(exchange, STATUS_INTERNAL_ERROR, "Internal Server Error".getBytes());
                } catch (IOException ex) {
                    exchange.close();
                }
            }
        });
    }

    private void processEntityRequest(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        String id = extractParam(query, PARAM_ID);
        int ack = extractAck(query);
        String method = exchange.getRequestMethod();

        if (isInvalidAck(ack)) {
            sendResponse(exchange, STATUS_BAD_REQUEST,
                    ("Invalid ack: " + ack + " > " + replicationFactor).getBytes());
            return;
        }

        if (id == null || id.isEmpty()) {
            sendResponse(exchange, STATUS_BAD_REQUEST, "Bad Request".getBytes());
            return;
        }

        sendAuditEvent(method, id);

        List<String> replicas = getReplicas(id);

        switch (method) {
            case METHOD_GET:
                handleReplicatedGet(exchange, id, replicas, ack);
                break;
            case METHOD_PUT:
                handleReplicatedPut(exchange, id, replicas, ack);
                break;
            case METHOD_DELETE:
                handleReplicatedDelete(exchange, id, replicas, ack);
                break;
            default:
                sendResponse(exchange, STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed".getBytes());
                break;
        }
    }

    private void sendAuditEvent(String method, String id) {
        AuditEvent event = new AuditEvent(method, id, System.currentTimeMillis());
        String value = event.method() + " " + event.id() + " " + event.timestamp();
        ProducerRecord<String, String> record = new ProducerRecord<>("audit", value);

        if (asyncMode) {
            try {
                KafkaProducer<String, String> producer = ensureProducer();
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        LOG.warn("Audit send failed: {}", exception.getMessage());
                    }
                });
            } catch (Exception e) {
                LOG.warn("Audit producer unavailable: {}", e.getMessage());
            }
        } else {
            try {
                KafkaProducer<String, String> producer = ensureProducer();
                producer.send(record).get();
            } catch (Exception e) {
                LOG.warn("Audit send failed: {}", e.getMessage());
            }
        }
    }

    private boolean isInvalidAck(int ack) {
        return ack > replicationFactor;
    }

    private List<String> getReplicas(String key) {
        return clusterNodes.stream()
                .sorted(Comparator.comparingLong((String node) -> {
                    int h1 = key.hashCode();
                    int h2 = node.hashCode();
                    return ((long) h1 << 32) | (h2 & 0xFFFFFFFFL);
                }).reversed())
                .limit(replicationFactor)
                .collect(Collectors.toList());
    }

    private boolean isReplicaDisabled(String replica) {
        for (int nodeId : disabledReplicas) {
            String expectedPrefix = "http://localhost:" + (port + nodeId);
            if (replica.startsWith(expectedPrefix)) {
                return true;
            }
        }
        return false;
    }

    private record ReplicaResponse(byte[] value, boolean found) {
    }

    private ReplicaResponse getFromReplica(String replica, String id) throws IOException {
        if (replica.startsWith(selfAddress)) {
            byte[] value = dao.get(id);
            return new ReplicaResponse(value, true);
        }
        return getFromReplicaGrpc(replica, id);
    }

    private void putToReplica(String replica, String id, byte[] body) throws IOException {
        if (replica.startsWith(selfAddress)) {
            dao.upsert(id, body);
            return;
        }
        putToReplicaGrpc(replica, id, body);
    }

    private void deleteFromReplica(String replica, String id) throws IOException {
        if (replica.startsWith(selfAddress)) {
            dao.delete(id);
            return;
        }
        deleteFromReplicaGrpc(replica, id);
    }

    private ReplicaResponse getFromReplicaGrpc(String replica, String id) {
        String[] parts = replica.split("\\?grpcPort=");
        String host = extractHost(parts[0]);
        int grpcPort = Integer.parseInt(parts[1]);

        ManagedChannel channel = grpcChannels.computeIfAbsent(replica,
                k -> ManagedChannelBuilder.forAddress(host, grpcPort).usePlaintext().build());

        KVInternalServiceGrpc.KVInternalServiceBlockingStub stub =
                KVInternalServiceGrpc.newBlockingStub(channel);

        GetRequest request = GetRequest.newBuilder().setId(id).build();
        GetResponse response = stub.get(request);

        if (response.getStatus() == STATUS_OK) {
            return new ReplicaResponse(response.getValue().toByteArray(), true);
        }
        return new ReplicaResponse(null, false);
    }

    private void putToReplicaGrpc(String replica, String id, byte[] body) {
        String[] parts = replica.split("\\?grpcPort=");
        String host = extractHost(parts[0]);
        int grpcPort = Integer.parseInt(parts[1]);

        ManagedChannel channel = grpcChannels.computeIfAbsent(replica,
                k -> ManagedChannelBuilder.forAddress(host, grpcPort).usePlaintext().build());

        KVInternalServiceGrpc.KVInternalServiceBlockingStub stub =
                KVInternalServiceGrpc.newBlockingStub(channel);

        PutRequest request = PutRequest.newBuilder()
                .setId(id)
                .setValue(com.google.protobuf.ByteString.copyFrom(body))
                .build();
        stub.put(request);
    }

    private void deleteFromReplicaGrpc(String replica, String id) {
        String[] parts = replica.split("\\?grpcPort=");
        String host = extractHost(parts[0]);
        int grpcPort = Integer.parseInt(parts[1]);

        ManagedChannel channel = grpcChannels.computeIfAbsent(replica,
                k -> ManagedChannelBuilder.forAddress(host, grpcPort).usePlaintext().build());

        KVInternalServiceGrpc.KVInternalServiceBlockingStub stub =
                KVInternalServiceGrpc.newBlockingStub(channel);

        DeleteRequest request = DeleteRequest.newBuilder().setId(id).build();
        stub.delete(request);
    }

    private String extractHost(String address) {
        return address.replace("http://", "").split(":")[0];
    }

    private void handleReplicatedGet(HttpExchange exchange, String id,
                                     List<String> replicas, int ack) throws IOException {
        List<byte[]> responses = new ArrayList<>();
        int successCount = 0;

        for (String replica : replicas) {
            try {
                if (isReplicaDisabled(replica)) {
                    continue;
                }
                ReplicaResponse response = getFromReplica(replica, id);
                if (response.found) {
                    responses.add(response.value.clone());
                }
                successCount++;
            } catch (NoSuchElementException e) {
                successCount++;
            } catch (Exception e) {
                // Replica unavailable
            }
        }

        if (successCount < ack) {
            sendResponse(exchange, STATUS_SERVICE_UNAVAILABLE,
                    ("Only " + successCount + " replicas available, need " + ack).getBytes());
            return;
        }

        if (responses.isEmpty()) {
            sendResponse(exchange, STATUS_NOT_FOUND, "Not Found".getBytes());
        } else {
            sendResponse(exchange, STATUS_OK, responses.getFirst());
        }
    }

    private void handleReplicatedPut(HttpExchange exchange, String id,
                                     List<String> replicas, int ack) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        int successCount = 0;

        for (String replica : replicas) {
            try {
                if (isReplicaDisabled(replica)) {
                    continue;
                }
                putToReplica(replica, id, body);
                successCount++;
            } catch (Exception e) {
                // Replica unavailable
            }
        }

        if (successCount < ack) {
            sendResponse(exchange, STATUS_SERVICE_UNAVAILABLE,
                    ("Only " + successCount + " replicas available, need " + ack).getBytes());
            return;
        }

        sendResponse(exchange, STATUS_CREATED, "Created".getBytes());
    }

    private void handleReplicatedDelete(HttpExchange exchange, String id,
                                        List<String> replicas, int ack) throws IOException {
        int successCount = 0;

        for (String replica : replicas) {
            try {
                if (isReplicaDisabled(replica)) {
                    continue;
                }
                deleteFromReplica(replica, id);
                successCount++;
            } catch (Exception e) {
                // Replica unavailable
            }
        }

        if (successCount < ack) {
            sendResponse(exchange, STATUS_SERVICE_UNAVAILABLE,
                    ("Only " + successCount + " replicas available, need " + ack).getBytes());
            return;
        }

        sendResponse(exchange, STATUS_ACCEPTED, "Accepted".getBytes());
    }

    private int extractAck(String query) {
        String ackStr = extractParam(query, PARAM_ACK);
        if (ackStr != null) {
            try {
                return Integer.parseInt(ackStr);
            } catch (NumberFormatException e) {
                return defaultAck;
            }
        }
        return defaultAck;
    }

    private String extractParam(String query, String paramName) {
        if (query == null) {
            return null;
        }
        for (String param : query.split("&")) {
            String[] pair = param.split("=", 2);
            if (pair.length == 2 && paramName.equals(pair[0])) {
                return pair[1];
            }
        }
        return null;
    }

    private void sendResponse(HttpExchange exchange, int code, byte[] body) throws IOException {
        exchange.sendResponseHeaders(code, body != null ? body.length : -1);
        if (body != null && body.length > 0) {
            exchange.getResponseBody().write(body);
        }
        exchange.close();
    }

    @Override
    public void start() {
        httpServer.start();
        try {
            grpcServer.start();
        } catch (IOException e) {
            throw new RuntimeException("Failed to start gRPC server", e);
        }
    }

    @Override
    public void stop() {
        httpServer.stop(0);
        grpcServer.stop();
        grpcChannels.values().forEach(channel -> {
            try {
                channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        KafkaProducer<String, String> p = kafkaProducer.get();
        if (p != null) {
            p.close();
        }
        try {
            dao.close();
        } catch (IOException ex) {
            // Closing DAO resource, exception can be safely ignored during shutdown
        }
    }
}
