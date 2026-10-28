package dev.campusdelivery.support;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * One PostGIS + one Kafka container for the whole test JVM (started once, reused by every Spring
 * context). Tests share the schema, so each creates its own users/merchants/orders.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "app.seed.enabled=false",
        "app.assignment.tick-ms=200",
        "app.assignment.offer-ttl=2s",
        "app.assignment.radius-meters=3000",
        "app.assignment.retry-cooldown=5m",
        "app.kafka.outbox-tick-ms=200",
        "app.location.stale-after=30s",
        "app.ws.replay-limit=20",
        "logging.level.dev.campusdelivery=INFO"
})
@Import(TestSupportConfig.class)
public abstract class AbstractIntegrationTest {

    @ServiceConnection
    protected static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(
            DockerImageName.parse("postgis/postgis:16-3.5").asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("campus").withUsername("campus").withPassword("campus");

    @ServiceConnection
    protected static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.2"));

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    @LocalServerPort
    protected int port;

    @Autowired
    protected Fixtures fx;

    @Autowired
    protected JdbcClient jdbc;

    protected String wsUrl(String token) {
        return "ws://localhost:" + port + "/ws?token=" + token;
    }
}
