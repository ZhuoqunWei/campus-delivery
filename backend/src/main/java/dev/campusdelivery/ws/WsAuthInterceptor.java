package dev.campusdelivery.ws;

import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

/**
 * Browsers cannot set headers on a WebSocket handshake, so the JWT travels as ?token=. The token is
 * short-lived and the handshake is the only request that carries it in the URL.
 */
@Component
public class WsAuthInterceptor implements HandshakeInterceptor {

    private static final Logger log = LoggerFactory.getLogger(WsAuthInterceptor.class);

    public static final String ATTR_USER_ID = "userId";
    public static final String ATTR_ROLE = "role";
    public static final String ATTR_NAME = "name";

    private final JwtDecoder decoder;

    public WsAuthInterceptor(JwtDecoder decoder) {
        this.decoder = decoder;
    }

    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response,
                                   WebSocketHandler wsHandler, Map<String, Object> attributes) {
        String token = null;
        if (request instanceof ServletServerHttpRequest servlet) {
            token = servlet.getServletRequest().getParameter("token");
        }
        if (token == null || token.isBlank()) {
            String header = request.getHeaders().getFirst("Authorization");
            if (header != null && header.startsWith("Bearer ")) {
                token = header.substring(7);
            }
        }
        if (token == null || token.isBlank()) {
            response.setStatusCode(HttpStatus.UNAUTHORIZED);
            return false;
        }
        try {
            Jwt jwt = decoder.decode(token);
            attributes.put(ATTR_USER_ID, UUID.fromString(jwt.getSubject()));
            attributes.put(ATTR_ROLE, jwt.getClaimAsString("role"));
            attributes.put(ATTR_NAME, jwt.getClaimAsString("name"));
            return true;
        } catch (JwtException | IllegalArgumentException e) {
            log.debug("WebSocket handshake rejected: {}", e.getMessage());
            response.setStatusCode(HttpStatus.UNAUTHORIZED);
            return false;
        }
    }

    @Override
    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response,
                               WebSocketHandler wsHandler, Exception exception) {
    }
}
