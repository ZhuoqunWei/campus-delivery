package dev.campusdelivery.merchants;

import dev.campusdelivery.common.GeoPoint;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class MerchantRepository {

    private static final String SELECT = """
            SELECT id, name, ST_Y(location::geometry) AS lat, ST_X(location::geometry) AS lng
            FROM merchants
            """;

    private static final RowMapper<MerchantView> MAPPER = (rs, i) -> new MerchantView(
            rs.getObject("id", UUID.class),
            rs.getString("name"),
            new GeoPoint(rs.getDouble("lat"), rs.getDouble("lng")));

    private final JdbcClient jdbc;

    public MerchantRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public List<MerchantView> findAll() {
        return jdbc.sql(SELECT + " ORDER BY name").query(MAPPER).list();
    }

    public Optional<MerchantView> findById(UUID id) {
        return jdbc.sql(SELECT + " WHERE id = :id").param("id", id).query(MAPPER).optional();
    }

    public UUID upsert(String name, GeoPoint location) {
        return jdbc.sql("""
                INSERT INTO merchants (name, location)
                VALUES (:name, ST_SetSRID(ST_MakePoint(:lng, :lat), 4326)::geography)
                ON CONFLICT (name) DO UPDATE SET location = EXCLUDED.location
                RETURNING id
                """)
                .param("name", name)
                .param("lat", location.lat())
                .param("lng", location.lng())
                .query(UUID.class)
                .single();
    }
}
