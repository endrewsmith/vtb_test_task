package ru.vtb.testask.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;
import ru.stubVTB.json.Req;
import ru.stubVTB.json.Res;
import ru.vtb.testask.properties.KafkaProperties;
import ru.vtb.testask.service.KafkaHealthChecker;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@RestController
@AllArgsConstructor
public class StubVTBController {

    private final KafkaHealthChecker kafkaHealthChecker;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ConsumerFactory<String, Object> consumerFactory;
    private final KafkaProperties properties;
    // Счетчик для генерации дополнительных событий
    private final AtomicInteger counter;

    @PostMapping(value = "/buy", produces = MediaType.APPLICATION_JSON_VALUE)
    public DeferredResult<ResponseEntity<?>> buyPost(@RequestBody String request) throws JsonProcessingException {
        int count = counter.incrementAndGet();
        String timestamp = Long.toString(Instant.now().getEpochSecond());
        log.debug("Message received from client: {}", request);
        DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult<>(5000L); // 5 sec timeout
        // Проверка на валидность request
        if (request == null || request.isEmpty()) {
            deferredResult.setResult(ResponseEntity.badRequest().build());
            return deferredResult;
        }

        ObjectMapper mapper = new ObjectMapper();
        Req req = mapper.readValue(request, Req.class);

        // Проверка на валидность поля price
        if (req == null || req.getPrice().isEmpty() || !isValidPrice(req.getPrice())) {
            deferredResult.setResult(ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Поле price введено неверно, либо отсутствует"));
            return deferredResult;
        }

        //  Если знаем тип price, например double, то можно проверить на переполнение
        try {
            Double.parseDouble(req.getPrice());
        } catch (Exception e) {
            deferredResult.setResult(ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Поле price введено неверно, либо отсутствует"));
            return deferredResult;
        }

        // Эмулируем случайную ошибку на сервере
        if (count % 100 == 0) {
            deferredResult.setResult(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Неизвестная ошибка сервера"));
            return deferredResult;
        }

        Res res = new Res();
        res.setPrice(req.getPrice());
        // Эмулируем ошибку на каждое 70ое сообщение, не вставляем поле MessageId
        if (count % 70 != 0) {
            res.setMessageId(UUID.randomUUID().toString());
        }
        res.setTimestamp(timestamp);
        res.setMethod("POST");
        res.setUri("/buy");

        ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
        String kafkaBody = objectWriter.writeValueAsString(res);
        var topic = properties.getTopic();
        try {
            if (kafkaHealthChecker.isKafkaHealthy(properties.getBootstrapServers())) {

                ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, kafkaBody);

                future.addCallback(new ListenableFutureCallback<>() {
                    @Override
                    public void onSuccess(SendResult<String, String> result) {
                        log.info("Message send to kafka: {}", kafkaBody);
                        deferredResult.setResult(ResponseEntity.status(HttpStatus.OK).body(res));
                    }

                    @Override
                    public void onFailure(Throwable ex) {
                        log.error("Message not send to kafka: {}", kafkaBody);
                        deferredResult.setResult(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Message not send to kafka"));
                    }
                });
            } else {
                log.error("Kafka not working");
                deferredResult.setResult(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Kafka not working"));
            }
        } catch (Exception e) {
            deferredResult.setResult(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Kafka not working"));
        }

        return deferredResult;
    }

    // Проверка поля
    public boolean isValidPrice(String price) {
        if (price == null) {
            return false;
        }
        return price.matches("^(0|[1-9]\\d*)(\\.\\d{1,2})?$");
    }

    @PostMapping(value = "/get-last-message-data", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> getLastMassageFromKafka() {

        log.info("Message received from client");

        try (Consumer<String, Object> consumer = consumerFactory.createConsumer()) {
            var topic = properties.getTopic();
            // Получаем информацию о партициях
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                log.error("Нет партиций для топика: {}", topic);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Проблемы с кафкой");
            }
            log.debug("LOG Количество партиций: {}", partitionInfos.size());
            // Получаем список партиций
            List<TopicPartition> partitionList = partitionInfos
                    .stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .collect(Collectors.toList());
            log.debug("Количество партиций для топика: {}, равно: {}", topic, partitionList.size());

            // Назначаем партиции вручную
            consumer.assign(partitionList);

            // Переходим к концу(последнему offset)
            consumer.seekToEnd(partitionList);

            // Читаем последнее сообщение из каждой партиции
            List<ConsumerRecord<String, Object>> lastRecords = new ArrayList<>();
            for (TopicPartition partition : partitionList) {
                long position = consumer.position(partition);
                if (position > 0) {
                    consumer.seek(partition, position - 1);
                    ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, Object> record : records) {
                        lastRecords.add(record);
                    }
                }
            }

            // Находим самое последнее сообщение
            if (!lastRecords.isEmpty()) {
                // Если точно знаем, что одна партиция, то сработает так
                ConsumerRecord<String, Object> lastRecord = lastRecords.stream()
                        .max(Comparator.comparingLong(ConsumerRecord::offset))
                        .orElse(null);
                // Если партиция не одна , то нужно по временному смещению
                // record.timestampType() == TimestampType.LOG_APPEND_TIME
                // ConsumerRecord<String, Object> lastRecord = lastRecords.stream()
                // .max(Comparator.comparingLong(ConsumerRecord::timestamp))
                // .orElse(null);
                if (lastRecord != null) {
                    log.info("Последнее сообщение в кафке с offset: {}, value: {}", lastRecord.offset(), lastRecord.value());
                    return ResponseEntity.status(HttpStatus.OK).body(lastRecord.value());
                }
            } else {
                log.info("В кафке нет сообщений для чтения");
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("В кафке нет сообщений для чтения");
            }
        } catch (Exception e) {
            log.error("Some error", e);
        }

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Ошибка при выгрузке последнего сообщения из кафки");
    }
}
