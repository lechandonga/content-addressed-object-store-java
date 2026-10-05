package com.github.lechandonga.contentaddressedobjectstorejava.store.config;

import com.github.lechandonga.contentaddressedobjectstorejava.store.api.ObjectStore;
import com.github.lechandonga.contentaddressedobjectstorejava.store.engine.LocalObjectStore;
import com.github.lechandonga.contentaddressedobjectstorejava.store.engine.StoreConfig;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Wires the local-disk object store from {@code objectstore.*} properties. */
@Configuration
@EnableConfigurationProperties(ObjectStoreProperties.class)
public class ObjectStoreConfiguration {

    @Bean
    public ObjectStore objectStore(ObjectStoreProperties properties) {
        StoreConfig config = new StoreConfig(
                properties.getDataDir().toAbsolutePath(),
                properties.getGcGracePeriod(),
                properties.getTempSweepMinAge());
        return new LocalObjectStore(config);
    }
}
