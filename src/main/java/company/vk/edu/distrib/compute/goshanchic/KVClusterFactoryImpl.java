package company.vk.edu.distrib.compute.goshanchic;

import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KVClusterFactoryImpl implements KVClusterFactory {

    @Override
    public KVCluster doCreate(List<Integer> ports) throws IOException {
        return new KVCluster() {
            private final List<KVService> services = new ArrayList<>();

            @Override
            public void start() throws IOException {
                InMemoryDao dao;
                KVService service;
                for (int port : ports) {
                    dao = new InMemoryDao();
                    service = new KVServiceImpl(port, ports, dao);
                    service.start();
                    services.add(service);
                }
            }

            @Override
            public void stop() {
                for (KVService service : services) {
                    service.stop();
                }
            }
        };
    }
}
