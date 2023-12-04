package ru.pandahouse.eulerdb.configuration;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;

@Configuration
public class RocksDbConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbConfiguration.class);

    private static final String DB_PATH = "/Users/maksimkozlov/rocksdb";

    @Bean
    public RocksDB rocksDb() throws RocksDBException {
        RocksDB.loadLibrary();

        File dbDir = new File(DB_PATH);

        Options options = new Options();
        options.setCreateIfMissing(true);

        long startMillis = System.currentTimeMillis();
        RocksDB db = RocksDB.open(options, dbDir.getAbsolutePath());
        long openMillis = System.currentTimeMillis();

        LOGGER.info("open DB {} ms", openMillis - startMillis);

        return db;
    }

}
