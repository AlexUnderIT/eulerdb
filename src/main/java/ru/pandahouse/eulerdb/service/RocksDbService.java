package ru.pandahouse.eulerdb.service;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;
import ru.pandahouse.eulerdb.repository.KVRepository;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Service
public class RocksDbService implements KVRepository<String, Object> {

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

    @Override
    public boolean save(String key, Object value) {
        LOGGER.info("Saving value '{}' with key '{}'", value, key);
        return addValue(key, value);
    }

    @Override
    public Optional<List<Object>> find(String key) {
        List<Object> value = new LinkedList<>();
        LOGGER.info("Trying to find value with key '{}'", key);
        try{
            byte[] bytes = rocksDB.get(key.getBytes(StandardCharsets.UTF_8));
            if (bytes != null) value = (List)(SerializationUtils.deserialize(bytes));
            LOGGER.info("Find value '{}' with key '{}'", value, key);
        } catch(RocksDBException e){
            LOGGER.error("Finding error. Cause: {} , message: {}", e.getCause(), e.getMessage());
        }
        return value.stream().findAny().isPresent() ? Optional.of(value) : Optional.empty();
    }

    @Override
    public boolean delete(String key) {
        LOGGER.info("Trying to delete value with key '{}'", key);
        try{
            rocksDB.delete(key.getBytes(StandardCharsets.UTF_8));
            LOGGER.info("Successfully deleted value with key '{}'", key);
        } catch(RocksDBException e){
            LOGGER.error("Deleting error. Cause: {} , message: {}", e.getCause(), e.getMessage());
            return false;
        }
        return true;
    }

    private boolean addValue(String key, Object value){
        List<Object> valueList = new LinkedList<>();
        Optional<List<Object>> findResult = find(key);
        if (findResult.isPresent()){
            valueList = findResult.get();
        }
        boolean present = valueList.stream().anyMatch(i -> i.equals(value));
        if(!present) valueList.add(value);
        try{
            rocksDB.put(key.getBytes(), SerializationUtils.serialize(valueList));
            LOGGER.info("Final value list: {}", valueList);
        }catch(RocksDBException e){
            LOGGER.error("Saving error. Cause: {} , message: {}", e.getCause(), e.getMessage());
            return false;
        }
        return true;
    }
}
