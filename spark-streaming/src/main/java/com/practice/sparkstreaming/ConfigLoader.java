package com.practice.sparkstreaming;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Loads spark-streaming.properties and resolves its ${VAR} placeholders against docker/.env,
 * the same secrets file application.yml reads via Spring's spring.config.import. docker/.env
 * happens to already be in KEY=VALUE form, so java.util.Properties can parse it directly.
 */
final class ConfigLoader {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([A-Za-z0-9_]+)}");
    private static final Path DOT_ENV = Path.of("docker/.env");

    private ConfigLoader() {
    }

    static Properties load() throws IOException {
        Properties raw = new Properties();
        try (InputStream in = ConfigLoader.class.getClassLoader()
                .getResourceAsStream("spark-streaming.properties")) {
            if (in == null) {
                throw new IOException("spark-streaming.properties not found on the classpath");
            }
            raw.load(in);
        }

        Properties secrets = new Properties();
        if (Files.isReadable(DOT_ENV)) {
            try (InputStream in = Files.newInputStream(DOT_ENV)) {
                secrets.load(in);
            }
        }

        Properties resolved = new Properties();
        for (String key : raw.stringPropertyNames()) {
            resolved.setProperty(key, resolvePlaceholders(raw.getProperty(key), secrets));
        }
        return resolved;
    }

    private static String resolvePlaceholders(String value, Properties secrets) {
        Matcher matcher = PLACEHOLDER.matcher(value);
        StringBuilder result = new StringBuilder();
        int lastEnd = 0;
        while (matcher.find()) {
            String varName = matcher.group(1);
            String replacement = secrets.getProperty(varName, System.getenv(varName));
            if (replacement == null) {
                throw new IllegalStateException(
                        "No value for ${" + varName + "} in docker/.env or the environment. "
                                + "Run docker/certs/generate-certs.sh first, and run this job "
                                + "from the repo root.");
            }
            result.append(value, lastEnd, matcher.start()).append(replacement);
            lastEnd = matcher.end();
        }
        result.append(value.substring(lastEnd));
        return result.toString();
    }
}
