package dev.campusdelivery.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** Minimal JDK WebSocket client that queues parsed JSON frames. */
public class WsTestClient implements WebSocket.Listener {

    private final ObjectMapper mapper = new ObjectMapper();
    private final LinkedBlockingQueue<JsonNode> queue = new LinkedBlockingQueue<>();
    private final StringBuilder partial = new StringBuilder();
    private final WebSocket ws;

    private WsTestClient(HttpClient client, String url) {
        this.ws = client.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .buildAsync(URI.create(url), this)
                .join();
    }

    public static WsTestClient connect(String url) {
        return new WsTestClient(HttpClient.newHttpClient(), url);
    }

    public void send(String json) {
        ws.sendText(json, true).join();
    }

    public void subscribe(Long lastEventId) {
        send(lastEventId == null ? "{\"type\":\"subscribe\"}" : "{\"type\":\"subscribe\",\"lastEventId\":" + lastEventId + "}");
    }

    /** Next frame of any type, or null on timeout. */
    public JsonNode next(Duration timeout) {
        try {
            return queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /** Skips frames of other types (e.g. location pings) until one of the wanted type arrives. */
    public JsonNode nextOfType(String type, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new AssertionError("Timed out waiting for WebSocket frame of type " + type);
            }
            JsonNode n = next(Duration.ofNanos(remaining));
            if (n == null) {
                throw new AssertionError("Timed out waiting for WebSocket frame of type " + type);
            }
            if (type.equals(n.path("type").asText())) {
                return n;
            }
        }
    }

    /** Like nextOfType but returns null instead of failing when the timeout passes; other frame types are ignored. */
    public JsonNode nextOfTypeOrNull(String type, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return null;
            }
            JsonNode n = next(Duration.ofNanos(remaining));
            if (n == null) {
                return null;
            }
            if (type.equals(n.path("type").asText())) {
                return n;
            }
        }
    }

    /** Collects consecutive "event" frames until a frame of `terminator` type arrives; returns [events..., terminator]. */
    public List<JsonNode> collectUntil(String terminator, Duration timeout) {
        List<JsonNode> out = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            long remaining = deadline - System.nanoTime();
            JsonNode n = remaining <= 0 ? null : next(Duration.ofNanos(remaining));
            if (n == null) {
                throw new AssertionError("Timed out waiting for " + terminator + "; got so far: " + out);
            }
            out.add(n);
            if (terminator.equals(n.path("type").asText())) {
                return out;
            }
        }
    }

    public void close() {
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "bye").join();
    }

    public void abort() {
        ws.abort();
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        partial.append(data);
        if (last) {
            try {
                queue.add(mapper.readTree(partial.toString()));
            } catch (Exception e) {
                throw new IllegalStateException("Bad frame: " + partial, e);
            }
            partial.setLength(0);
        }
        webSocket.request(1);
        return null;
    }
}
