package dev.campusdelivery.users;

import java.util.UUID;

public record UserRecord(UUID id, String email, String passwordHash, String displayName, UserRole role) {

    public UserView toView() {
        return new UserView(id, email, displayName, role);
    }

    public record UserView(UUID id, String email, String name, UserRole role) {}
}
