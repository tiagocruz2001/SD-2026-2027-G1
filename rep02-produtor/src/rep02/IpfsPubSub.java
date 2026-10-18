package rep02;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class IpfsPubSub {
    private final String api; private final HttpClient http = HttpClient.newHttpClient();
    public IpfsPubSub(String api) { this.api = api.endsWith("/") ? api.substring(0, api.length() - 1) : api; }
    static String toMultibase(String value) { return "u" + Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8)); }
    public void publish(String topic, String message) throws IOException, InterruptedException { String boundary = "rep02-" + System.nanoTime(); String body = "--" + boundary + "\r\nContent-Disposition: form-data; name=\"file\"; filename=\"msg\"\r\nContent-Type: application/octet-stream\r\n\r\n" + message + "\r\n--" + boundary + "--\r\n"; HttpRequest request = HttpRequest.newBuilder(URI.create(api + "/pubsub/pub?arg=" + toMultibase(topic))).header("Content-Type", "multipart/form-data; boundary=" + boundary).POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build(); HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString()); if (response.statusCode() != 200) throw new IOException("pub falhou (" + response.statusCode() + "): " + response.body()); }
}
