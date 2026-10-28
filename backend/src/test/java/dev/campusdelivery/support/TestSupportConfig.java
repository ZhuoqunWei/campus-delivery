package dev.campusdelivery.support;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class TestSupportConfig {

    @Bean
    Fixtures fixtures(dev.campusdelivery.users.UserRepository users,
                      dev.campusdelivery.merchants.MerchantRepository merchants,
                      org.springframework.security.crypto.password.PasswordEncoder encoder,
                      org.springframework.boot.test.web.client.TestRestTemplate rest,
                      com.fasterxml.jackson.databind.ObjectMapper mapper) {
        return new Fixtures(users, merchants, encoder, rest, mapper);
    }
}
