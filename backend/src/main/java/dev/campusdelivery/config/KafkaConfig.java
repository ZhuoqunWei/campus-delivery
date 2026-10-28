package dev.campusdelivery.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Topics are versioned in their names; message bodies carry schemaVersion too (see contracts/). */
@Configuration
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true")
public class KafkaConfig {

    @Bean
    public NewTopic orderEventsTopic(AppProperties props) {
        return TopicBuilder.name(props.kafka().orderEventsTopic()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic courierEventsTopic(AppProperties props) {
        return TopicBuilder.name(props.kafka().courierEventsTopic()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic courierLocationsTopic(AppProperties props) {
        return TopicBuilder.name(props.kafka().courierLocationsTopic()).partitions(6).replicas(1).build();
    }
}
