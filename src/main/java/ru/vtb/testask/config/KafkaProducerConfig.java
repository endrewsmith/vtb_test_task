package ru.vtb.testask.config;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.core.ProducerFactory;
import ru.vtb.testask.properties.KafkaProperties;

import java.util.concurrent.atomic.AtomicInteger;

@Configuration
@AllArgsConstructor
public class KafkaProducerConfig {
    private final KafkaProperties properties;
    private final MeterRegistry meterRegistry;

    @Bean
    public ProducerFactory<String, String> producerFactory(org.springframework.boot.autoconfigure.kafka.KafkaProperties kafkaProperties) {
        var configProps = kafkaProperties.buildConsumerProperties();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        configProps.put(ProducerConfig.CLIENT_ID_CONFIG, properties.getClientId());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Количество попыток отправки сообщения при сбое
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Время ожидания перед следующей попыткой (в миллисекундах)
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        // Добавление слушателя для сбора метрик
        DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(configProps);
        factory.addListener(new MicrometerProducerListener<>(meterRegistry));
        return factory;
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public AtomicInteger atomicInteger() {
        return new AtomicInteger(0);
    }
}
