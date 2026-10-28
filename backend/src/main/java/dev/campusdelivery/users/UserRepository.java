package dev.campusdelivery.users;

import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class UserRepository {

    private static final RowMapper<UserRecord> MAPPER = (rs, i) -> new UserRecord(
            rs.getObject("id", UUID.class),
            rs.getString("email"),
            rs.getString("password_hash"),
            rs.getString("display_name"),
            UserRole.valueOf(rs.getString("role")));

    private final JdbcClient jdbc;

    public UserRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<UserRecord> findByEmail(String email) {
        return jdbc.sql("SELECT id, email, password_hash, display_name, role::text AS role FROM users WHERE email = :email")
                .param("email", email.toLowerCase())
                .query(MAPPER)
                .optional();
    }

    public Optional<UserRecord> findById(UUID id) {
        return jdbc.sql("SELECT id, email, password_hash, display_name, role::text AS role FROM users WHERE id = :id")
                .param("id", id)
                .query(MAPPER)
                .optional();
    }

    /** Idempotent insert; returns the id whether or not the user existed. Couriers also get their courier row. */
    public UUID upsert(String email, String passwordHash, String displayName, UserRole role) {
        UUID id = jdbc.sql("""
                INSERT INTO users (email, password_hash, display_name, role)
                VALUES (:email, :hash, :name, CAST(:role AS user_role))
                ON CONFLICT (email) DO UPDATE SET email = EXCLUDED.email
                RETURNING id
                """)
                .param("email", email.toLowerCase())
                .param("hash", passwordHash)
                .param("name", displayName)
                .param("role", role.name())
                .query(UUID.class)
                .single();
        if (role == UserRole.COURIER) {
            jdbc.sql("INSERT INTO couriers (user_id) VALUES (:id) ON CONFLICT DO NOTHING")
                    .param("id", id)
                    .update();
        }
        return id;
    }
}
