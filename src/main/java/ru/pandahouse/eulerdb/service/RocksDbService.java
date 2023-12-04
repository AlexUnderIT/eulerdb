package ru.pandahouse.eulerdb.service;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Service
public class RocksDbService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbService.class);

    private final RocksDB rocksDB;

    @Autowired
    public RocksDbService(
        RocksDB rocksDB
    ) {
        this.rocksDB = rocksDB;
    }

    public void get() {
        try {
            long startMillis = System.currentTimeMillis();
            LOGGER.info("method start");

            String uuid = UUID.randomUUID().toString();

            WriteOptions writeOptions = new WriteOptions();
            byte[] bytes = uuid.getBytes(StandardCharsets.UTF_8);
            rocksDB.put(writeOptions, bytes, bytes);
//            rocksDB.merge();
            long putMillis = System.currentTimeMillis();
            LOGGER.info("open {} ms", putMillis - startMillis);

            rocksDB.get(bytes);
            long getMillis = System.currentTimeMillis();
            LOGGER.info("open {} ms", getMillis - putMillis);

        } catch (RocksDBException e) {
            LOGGER.info(e.getMessage(), e);
        }
    }

}
