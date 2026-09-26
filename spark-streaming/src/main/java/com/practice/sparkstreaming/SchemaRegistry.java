package com.practice.sparkstreaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Fetches a subject's latest schema (both its numeric ID and its JSON) from the Confluent Schema
 * Registry's plain REST API (unauthenticated in this stack, see docker-compose.yml). Deliberately
 * a plain HttpClient call rather than io.confluent:kafka-schema-registry-client, to avoid pulling
 * in a second, differently versioned copy of Jackson alongside the one Spark already bundles.
 */
final class SchemaRegistry {

    record SchemaInfo(int id, String schema) {
    }

    private SchemaRegistry() {
    }

    static SchemaInfo fetchLatest(String registryUrl, String subject) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(registryUrl + "/subjects/" + subject + "/versions/latest"))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() != 200) {
            throw new IOException("Schema Registry GET " + request.uri() + " returned "
                    + response.statusCode() + ": " + response.body());
        }
        JsonNode body = new ObjectMapper().readTree(response.body());
        return new SchemaInfo(body.get("id").asInt(), body.get("schema").asText());
    }
}
