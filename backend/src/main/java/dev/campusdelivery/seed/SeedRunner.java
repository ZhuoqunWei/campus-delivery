package dev.campusdelivery.seed;

import dev.campusdelivery.common.GeoPoint;
import dev.campusdelivery.config.AppProperties;
import dev.campusdelivery.merchants.MerchantRepository;
import dev.campusdelivery.users.UserRepository;
import dev.campusdelivery.users.UserRole;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Idempotent demo data: merchants around the campus center, one dispatcher, a few customers and a
 * pool of couriers (the simulator logs in as these). All passwords = app.seed.password.
 */
@Component
@ConditionalOnProperty(prefix = "app.seed", name = "enabled", havingValue = "true")
public class SeedRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(SeedRunner.class);

    private record MerchantSeed(String name, double dLat, double dLng) {}

    private static final List<MerchantSeed> MERCHANTS = List.of(
            new MerchantSeed("Library Café", 0.0012, -0.0018),
            new MerchantSeed("Union Grill", -0.0021, 0.0009),
            new MerchantSeed("Engineering Quad Boba", 0.0030, 0.0035),
            new MerchantSeed("North Gate Pizza", 0.0048, -0.0007),
            new MerchantSeed("Southside Sushi", -0.0052, 0.0022),
            new MerchantSeed("Stadium Tacos", -0.0015, 0.0061));

    private final UserRepository users;
    private final MerchantRepository merchants;
    private final PasswordEncoder encoder;
    private final AppProperties props;

    public SeedRunner(UserRepository users, MerchantRepository merchants, PasswordEncoder encoder, AppProperties props) {
        this.users = users;
        this.merchants = merchants;
        this.encoder = encoder;
        this.props = props;
    }

    @Override
    @Transactional
    public void run(ApplicationArguments args) {
        AppProperties.Campus campus = props.campus();
        for (MerchantSeed m : MERCHANTS) {
            merchants.upsert(m.name(), new GeoPoint(campus.lat() + m.dLat(), campus.lng() + m.dLng()));
        }
        String hash = encoder.encode(props.seed().password());
        users.upsert("dispatcher@campus.test", hash, "Dispatcher Dana", UserRole.DISPATCHER);
        for (int i = 1; i <= props.seed().customers(); i++) {
            users.upsert("customer" + i + "@campus.test", hash, "Customer " + i, UserRole.CUSTOMER);
        }
        for (int i = 1; i <= props.seed().couriers(); i++) {
            users.upsert("courier" + i + "@campus.test", hash, "Courier " + i, UserRole.COURIER);
        }
        log.info("Seeded {} merchants, 1 dispatcher, {} customers, {} couriers around {} ({}, {})",
                MERCHANTS.size(), props.seed().customers(), props.seed().couriers(),
                campus.name(), campus.lat(), campus.lng());
    }
}
