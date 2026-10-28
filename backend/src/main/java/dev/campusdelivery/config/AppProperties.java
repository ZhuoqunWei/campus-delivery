package dev.campusdelivery.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** All tunables in one place. Defaults live in application.yml. */
@ConfigurationProperties(prefix = "app")
public record AppProperties(
        Jwt jwt,
        Campus campus,
        Assignment assignment,
        Location location,
        Dispatch dispatch,
        Ws ws,
        Kafka kafka,
        Seed seed) {

    public record Jwt(String secret, Duration ttl) {}

    public record Campus(String name, double lat, double lng) {}

    public record Assignment(long tickMs, Duration offerTtl, double radiusMeters,
                             Duration courierFreshness, Duration retryCooldown) {}

    public record Location(Duration staleAfter, int maxBatch, Duration maxFutureSkew) {}

    public record Dispatch(Duration stuckAfter) {}

    public record Ws(int replayLimit) {}

    public record Kafka(boolean enabled, long outboxTickMs, String orderEventsTopic,
                        String courierEventsTopic, String courierLocationsTopic) {}

    public record Seed(boolean enabled, int customers, int couriers, String password) {}
}
