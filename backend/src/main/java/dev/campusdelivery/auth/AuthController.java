package dev.campusdelivery.auth;

import dev.campusdelivery.common.ApiException;
import dev.campusdelivery.users.UserRecord;
import dev.campusdelivery.users.UserRepository;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import java.util.UUID;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private final UserRepository users;
    private final PasswordEncoder passwordEncoder;
    private final TokenService tokens;

    public AuthController(UserRepository users, PasswordEncoder passwordEncoder, TokenService tokens) {
        this.users = users;
        this.passwordEncoder = passwordEncoder;
        this.tokens = tokens;
    }

    public record LoginRequest(@NotBlank @Email String email, @NotBlank String password) {}

    public record LoginResponse(String token, UserRecord.UserView user) {}

    @PostMapping("/login")
    public LoginResponse login(@Valid @RequestBody LoginRequest req) {
        UserRecord user = users.findByEmail(req.email())
                .filter(u -> passwordEncoder.matches(req.password(), u.passwordHash()))
                .orElseThrow(() -> ApiException.unauthorized("Invalid email or password"));
        return new LoginResponse(tokens.issue(user), user.toView());
    }

    @GetMapping("/me")
    public UserRecord.UserView me(@AuthenticationPrincipal Jwt jwt) {
        return users.findById(UUID.fromString(jwt.getSubject()))
                .map(UserRecord::toView)
                .orElseThrow(() -> ApiException.notFound("User"));
    }
}
