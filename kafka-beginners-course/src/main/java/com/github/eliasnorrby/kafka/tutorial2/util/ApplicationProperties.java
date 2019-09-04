package com.github.eliasnorrby.kafka.tutorial2.util;

import java.io.FileInputStream;
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

    public String getConsumerKey() {
        return properties.getProperty("consumerKey");
    }
    public String getConsumerSecret() {
        return properties.getProperty("consumerSecret");
    }
    public String getToken() {
        return properties.getProperty("token");
    }
    public String getSecret() {
        return properties.getProperty("secret");
    }
}
