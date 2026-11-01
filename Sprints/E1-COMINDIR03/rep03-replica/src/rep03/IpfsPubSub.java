package rep03;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class IpfsPubSub {
    private final String api;
    private final HttpClient http = HttpClient.newHttpClient();

    public IpfsPubSub(String api) {
        this.api = api.endsWith("/") ? api.substring(0, api.length() - 1) : api;
    }

    static String toMultibase(String value) {
        return "u" + Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    static String fromMultibase(String value) {
        if (!value.startsWith("u"))
            throw new IllegalArgumentException("Codificacao multibase invalida");
        return new String(Base64.getUrlDecoder().decode(value.substring(1)), StandardCharsets.UTF_8);
    }

    public void subscribe(String topic, Consumer<String> onMessage) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(api + "/pubsub/sub?arg=" + toMultibase(topic)))
                .POST(HttpRequest.BodyPublishers.noBody()).build();
        HttpResponse<Stream<String>> response = http.send(request, HttpResponse.BodyHandlers.ofLines());
        if (response.statusCode() != 200)
            throw new IOException("sub falhou (" + response.statusCode() + ")");
        try (Stream<String> lines = response.body()) {
            lines.filter(line -> line.contains("\"data\""))
                    .forEach(line -> onMessage.accept(fromMultibase(extractField(line, "data"))));
        }
    }

    static String extractField(String json, String field) {
        String key = "\"" + field + "\":\"";
        int start = json.indexOf(key);
        if (start < 0)
            throw new IllegalArgumentException("Campo ausente: " + field);
        start += key.length();
        int end = json.indexOf('"', start);
        if (end < 0)
            throw new IllegalArgumentException("Campo incompleto: " + field);
        return json.substring(start, end);
    }
}
