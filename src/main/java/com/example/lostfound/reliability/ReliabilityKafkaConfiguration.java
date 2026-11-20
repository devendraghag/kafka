package com.example.lostfound.reliability;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.FixedBackOff;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class ReliabilityKafkaConfiguration {
    @Bean NewTopic emailDeadLetterTopic(@Value("${app.kafka.email-dlt}") String topic) {
        return TopicBuilder.name(topic).partitions(3).replicas(1).build();
    }
    @Bean NewTopic transactionalAuditTopic(@Value("${app.kafka.transaction-output-topic}") String topic) {
        return TopicBuilder.name(topic).partitions(3).replicas(1).build();
    }
    @Bean NewTopic transactionalEmailTopic(@Value("${app.kafka.transaction-email-topic}") String topic) {
        return TopicBuilder.name(topic).partitions(3).replicas(1).build();
    }

    @Bean DefaultErrorHandler emailRetryErrorHandler(@Qualifier("kafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${app.kafka.email-dlt}") String dltTopic,
            @Value("${app.notifications.email.ack-mode:record}") String ackMode) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new org.apache.kafka.common.TopicPartition(dltTopic, record.partition()));
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(500L, 2L));
        handler.setRetryListeners((record, exception, deliveryAttempt) ->
                org.slf4j.LoggerFactory.getLogger(getClass()).warn(
                        "Email retry attempt={} topic={} partition={} offset={} cause={}", deliveryAttempt,
                        record.topic(), record.partition(), record.offset(), exception.toString()));
        if ("manual_immediate".equalsIgnoreCase(ackMode)) handler.setCommitRecovered(true);
        return handler;
    }

    @Bean(name = "emailRetryListenerContainerFactory")
    @SuppressWarnings({"rawtypes", "unchecked"})
    ConcurrentKafkaListenerContainerFactory emailRetryListenerContainerFactory(ConsumerFactory consumerFactory,
            DefaultErrorHandler emailRetryErrorHandler, @Value("${app.notifications.email.ack-mode:record}") String ackMode) {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(emailRetryErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.valueOf(ackMode.toUpperCase()));
        return factory;
    }

    @Bean(name = "kafkaTemplate")
    @SuppressWarnings({"rawtypes", "unchecked"})
    KafkaTemplate<String, Object> kafkaTemplate(@Qualifier("kafkaProducerFactory") ProducerFactory producerFactory) {
        return new KafkaTemplate(producerFactory);
    }

    @Bean(name = "transactionalProducerFactory")
    ProducerFactory<String, Object> transactionalProducerFactory(KafkaProperties kafkaProperties,
            @Value("${KAFKA_TRANSACTION_ID_PREFIX:notification-tx-${random.uuid}-}") String prefix) {
        Map<String, Object> properties = new HashMap<>(kafkaProperties.buildProducerProperties());
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(properties);
        factory.setTransactionIdPrefix(prefix);
        return factory;
    }

    @Bean(name = "transactionalKafkaTemplate")
    KafkaTemplate<String, Object> transactionalKafkaTemplate(
            @Qualifier("transactionalProducerFactory") ProducerFactory<String, Object> factory) {
        return new KafkaTemplate<>(factory);
    }

    @Bean(name = "kafkaTransactionManager")
    KafkaTransactionManager<String, Object> kafkaTransactionManager(
            @Qualifier("transactionalProducerFactory") ProducerFactory<String, Object> factory) {
        return new KafkaTransactionManager<>(factory);
    }

    @Bean(name = "transactionalListenerContainerFactory")
    @SuppressWarnings({"rawtypes", "unchecked"})
    ConcurrentKafkaListenerContainerFactory transactionalListenerContainerFactory(ConsumerFactory consumerFactory,
            @Qualifier("kafkaTransactionManager") KafkaTransactionManager transactionManager) {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setKafkaAwareTransactionManager(transactionManager);
        return factory;
    }
}