package dev.campusdelivery.ws;

import com.fasterxml.jackson.databind.JsonNode;
import dev.campusdelivery.common.Json;
import dev.campusdelivery.events.EventRecord;
import dev.campusdelivery.events.LiveEventBus;
import dev.campusdelivery.events.LiveListener;
import dev.campusdelivery.events.LocationUpdate;
import dev.campusdelivery.users.UserRole;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * Protocol (JSON text frames):
 *   client -> {"type":"subscribe","lastEventId":123}      (lastEventId optional)
 *   client -> {"type":"ping"}
 *   server -> {"type":"hello", ...}
 *   server -> {"type":"snapshot","lastEventId":N,"orders":[...],"couriers":[...]}
 *   server -> {"type":"event","id":N,"eventType":"ORDER_ACCEPTED","at":"...","payload":{...}}
 *   server -> {"type":"resumed","lastEventId":N,"replayed":k,"couriers":[...]}
 *   server -> {"type":"location","courierId":"...","lat":..,"lng":..,"recordedAt":"...","orderId":..}
 *   server -> {"type":"pong"} | {"type":"error","message":"..."}
 * Live events that arrive while a replay is in flight are held back and flushed afterwards, so the
 * client always sees ids in ascending order on one connection.
 */
@Component
public class LiveFeedHandler extends TextWebSocketHandler implements LiveListener {

    private static final Logger log = LoggerFactory.getLogger(LiveFeedHandler.class);

    private enum State { CONNECTED, SYNCING, LIVE }

    private static final class Client {
        final WebSocketSession session;
        final Scope scope;
        State state = State.CONNECTED;
        long lastSentId = 0;
        final Deque<Object> backlog = new ArrayDeque<>();

        Client(WebSocketSession session, Scope scope) {
            this.session = session;
            this.scope = scope;
        }
    }

    private final Map<String, Client> clients = new ConcurrentHashMap<>();
    private final LiveFeedService feed;
    private final LiveEventBus bus;
    private final Json json;
    private final Clock clock;

    public LiveFeedHandler(LiveFeedService feed, LiveEventBus bus, Json json, Clock clock) {
        this.feed = feed;
        this.bus = bus;
        this.json = json;
        this.clock = clock;
    }

    @PostConstruct
    void register() {
        bus.register(this);
    }

    public int connectedClients() {
        return clients.size();
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession raw) {
        WebSocketSession session = new ConcurrentWebSocketSessionDecorator(raw, 10_000, 2 * 1024 * 1024);
        Scope scope = new Scope(
                UserRole.valueOf((String) raw.getAttributes().get(WsAuthInterceptor.ATTR_ROLE)),
                (UUID) raw.getAttributes().get(WsAuthInterceptor.ATTR_USER_ID));
        Client client = new Client(session, scope);
        clients.put(raw.getId(), client);
        Map<String, Object> hello = new LinkedHashMap<>();
        hello.put("type", "hello");
        hello.put("role", scope.role());
        hello.put("userId", scope.userId());
        hello.put("serverTime", clock.instant());
        send(client, hello);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        Client client = clients.get(session.getId());
        if (client == null) {
            return;
        }
        JsonNode msg;
        try {
            msg = json.read(message.getPayload());
        } catch (RuntimeException e) {
            send(client, Map.of("type", "error", "message", "invalid JSON"));
            return;
        }
        switch (msg.path("type").asText("")) {
            case "subscribe" -> subscribe(client, msg.hasNonNull("lastEventId") ? msg.get("lastEventId").asLong() : null);
            case "ping" -> send(client, Map.of("type", "pong", "serverTime", clock.instant()));
            default -> send(client, Map.of("type", "error", "message", "unknown message type"));
        }
    }

    private void subscribe(Client client, Long lastEventId) {
        synchronized (client) {
            client.state = State.SYNCING;
            client.backlog.clear();
        }
        LiveFeedService.Sync sync;
        try {
            sync = feed.resume(client.scope, lastEventId);
        } catch (RuntimeException e) {
            log.warn("Subscribe failed for {}", client.scope, e);
            send(client, Map.of("type", "error", "message", "subscribe failed"));
            synchronized (client) {
                client.state = State.CONNECTED;
            }
            return;
        }
        synchronized (client) {
            for (Map<String, Object> m : sync.messages()) {
                send(client, m);
            }
            client.lastSentId = sync.lastEventId();
            // Anything that committed during the sync and is newer than the high-water mark goes out now.
            for (Object held : client.backlog) {
                if (held instanceof EventRecord e) {
                    if (e.id() > client.lastSentId) {
                        send(client, eventMessage(e));
                        client.lastSentId = e.id();
                    }
                } else {
                    send(client, held);
                }
            }
            client.backlog.clear();
            client.state = State.LIVE;
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        clients.remove(session.getId());
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        log.debug("WebSocket transport error on {}: {}", session.getId(), exception.getMessage());
        clients.remove(session.getId());
    }

    // ---- LiveListener (called after commit, on the writer's thread) ----

    @Override
    public void onEvent(EventRecord event) {
        for (Client client : clients.values()) {
            if (!client.scope.matches(event)) {
                continue;
            }
            synchronized (client) {
                switch (client.state) {
                    case LIVE -> {
                        if (event.id() > client.lastSentId) {
                            send(client, eventMessage(event));
                            client.lastSentId = event.id();
                        }
                    }
                    case SYNCING -> client.backlog.add(event);
                    case CONNECTED -> { /* not subscribed yet */ }
                }
            }
        }
    }

    @Override
    public void onLocation(LocationUpdate update) {
        Map<String, Object> msg = locationMessage(update);
        for (Client client : clients.values()) {
            if (!client.scope.matches(update)) {
                continue;
            }
            synchronized (client) {
                switch (client.state) {
                    case LIVE -> send(client, msg);
                    case SYNCING -> client.backlog.add(msg);
                    case CONNECTED -> { /* not subscribed yet */ }
                }
            }
        }
    }

    // ---- message shapes ----

    static Map<String, Object> eventMessage(EventRecord e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", "event");
        m.put("id", e.id());
        m.put("eventType", e.type());
        m.put("at", e.createdAt());
        m.put("orderId", e.orderId());
        m.put("courierId", e.courierId());
        m.put("payload", e.payload());
        return m;
    }

    static Map<String, Object> locationMessage(LocationUpdate u) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", "location");
        m.put("courierId", u.courierId());
        m.put("lat", u.lat());
        m.put("lng", u.lng());
        m.put("recordedAt", u.recordedAt());
        m.put("receivedAt", u.receivedAt());
        m.put("orderId", u.orderId());
        return m;
    }

    private void send(Client client, Object payload) {
        try {
            client.session.sendMessage(new TextMessage(json.write(payload)));
        } catch (IOException | IllegalStateException e) {
            log.debug("Dropping WebSocket client {}: {}", client.session.getId(), e.getMessage());
            clients.remove(client.session.getId());
            try {
                client.session.close(CloseStatus.SESSION_NOT_RELIABLE);
            } catch (IOException ignored) {
                // already gone
            }
        }
    }
}
