package ru.vtb.testask.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaHealthChecker {
    public boolean isKafkaHealthy(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");

        try (AdminClient admin = AdminClient.create(props)) {
            DescribeClusterOptions options = new DescribeClusterOptions().timeoutMs(5000);
            admin.describeCluster(options).clusterId().get();
            return true;
        } catch (InterruptedException | ExecutionException e) {
            return false;
        }
    }
}
