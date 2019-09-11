package com.github.eliasnorrby.kafka.tutorial3.util;

import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public enum ApplicationProperties {
    INSTANCE;

    private final Properties properties;

    ApplicationProperties() {
        properties = new Properties();
        try {
            properties.load(getClass().getClassLoader().getResourceAsStream("config.properties"));
        } catch (IOException e) {
            Logger.getLogger(getClass().getName()).log(Level.SEVERE, e.getMessage(), e);
        }
    }

    public String getHostname() {
        return properties.getProperty("hostname");
    }
    public String getUsername() {
        return properties.getProperty("username");
    }
    public String getPassword() {
        return properties.getProperty("password");
    }
}
