package ru.pandahouse.eulerdb.service;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;
import ru.pandahouse.eulerdb.repository.KVRepository;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class RocksDbService implements KVRepository<String, Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbService.class);

    private final RocksDB rocksDB;
    private final List<ColumnFamilyHandle> columnFamilyHandleList;

    @Autowired
    public RocksDbService(
            RocksDB rocksDB,
            List<ColumnFamilyHandle> columnFamilyHandleList
    ) {
        this.rocksDB = rocksDB;
        this.columnFamilyHandleList = columnFamilyHandleList;
    }

    @Override
    public boolean save(String key, Object value) {
        LOGGER.info("---[DB] Operation PUT. Value: [{}], Key: [{}]---", value, key);
        try{
            rocksDB.put(key.getBytes(), SerializationUtils.serialize(value));
        } catch(RocksDBException e){
            LOGGER.error("---[ERROR] PUT error. Cause: {} , message: {}---", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public Optional<List<Object>> find(String key) {
        List<Object> value = new LinkedList<>();
        LOGGER.info("---[DB] Operation GET. Key: [{}].", key);
        try {
            byte[] bytes = rocksDB.get(key.getBytes(StandardCharsets.UTF_8));
            if (bytes != null) value = new LinkedList<>(List.of(Objects.requireNonNull(SerializationUtils.deserialize(bytes))));
            LOGGER.info("---[DB] GET value {} with key [{}].", value, key);
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] GET error. Cause: {} , message: [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return value.stream().findAny().isPresent() ? Optional.of(value) : Optional.empty();
    }

    @Override
    public boolean delete(String key) {
        LOGGER.info("---[DB] Operation DELETE. Key: [{}].", key);
        try {
            rocksDB.delete(key.getBytes(StandardCharsets.UTF_8));
            LOGGER.info("---[DB] DELETE value with key [{}].", key);
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] DELETE error. Cause: [{}] , message: [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return true;
    }
    @Override
    public boolean add(String key, Object value) {
        LOGGER.info("---[DB] Operation ADD. Key: [{}], value [{}]", key, value);
        return addValue(key, value);
    }

    public Optional<List<Object>> findMultipleValues(List<String> keyList) {
        List<Object> valueList = new ArrayList<>();
        List<byte[]> keyByteList = keyList.stream().map(i -> i.getBytes(StandardCharsets.UTF_8)).collect(Collectors.toList());
        LOGGER.info("---[DB] Operation GET WITH MULTIPLE KEYS. Key list: {}.", keyList);
        try {
            List<byte[]> valueByteList = rocksDB.multiGetAsList(keyByteList);
            if (!valueByteList.isEmpty()) {
                valueList = valueByteList.stream().map(SerializationUtils::deserialize).collect(Collectors.toList());
            }
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] GET WITH MULTIPLE KEYS error. Cause: {} , message: {}---", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        LOGGER.info("Values for this list: {}", valueList);
        return valueList.isEmpty() ? Optional.empty() : Optional.of(valueList);
    }

    //Удаляет в диапазоне [beginKey, endKey), не включая значения endKey
    public boolean deleteMultipleKeys(String beginKey, String endKey) {
        LOGGER.info("---[DB] Operation DELETE WITH MULTIPLE KEYS. Begin key: [{}], end key (not included)   : [{}]", beginKey, endKey);
        try {
            rocksDB.deleteRange(beginKey.getBytes(), endKey.getBytes());
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] DELETE WITH MULTIPLE KEYS error. Cause: {} , message: {}.", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return true;
    }

    public boolean saveColumnFamily(String columnFamilyHandleName, String key, Object value) {
        ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandleByName(columnFamilyHandleName.getBytes(StandardCharsets.UTF_8));
        if (columnFamilyHandle == null) {
            LOGGER.error("---[ERROR] ColumnFamilyHandle with name [{}] didn't find.", columnFamilyHandleName);
            return false;
        }
        try {
            rocksDB.put(columnFamilyHandle, key.getBytes(StandardCharsets.UTF_8), SerializationUtils.serialize(value));
            LOGGER.info("---[DB] PUT value [{}] with key [{}] in ColumnFamily [{}].", value, key, columnFamilyHandleName);
            return true;
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] ColFam PUT ERROR. Cause: [{}], message: [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public Object findColumnFamilyValue(String columnFamilyHandleName, String key) {
        ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandleByName(columnFamilyHandleName.getBytes(StandardCharsets.UTF_8));
        Object result = new Object();
        try {
            byte[] resultByte = rocksDB.get(columnFamilyHandle, key.getBytes());
            if (resultByte != null) {
                LOGGER.info("---[DB] GET value [{}] with key [{}] from ColumnFamily [{}].", SerializationUtils.deserialize(resultByte), key, columnFamilyHandleName);
                result = SerializationUtils.deserialize(resultByte);
            } else {
                LOGGER.info("---[DB] Value with such key didn't find.---");
            }
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] ColFam GET ERROR. Cause: [{}], message: [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return result;
    }

    public boolean deleteColumnFamilyValue(String columnFamilyHandleName, String key) {
        ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandleByName(columnFamilyHandleName.getBytes());
        try {
            rocksDB.delete(columnFamilyHandle, key.getBytes(StandardCharsets.UTF_8));
            LOGGER.info("---[DB] DELETE value with key [{}] from ColumnFamily [{}].", key, columnFamilyHandleName);
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] ColFam DELETE error. Cause: [{}], message: [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e    );
        }
        return true;
    }

    public Optional<List<Object>> findMultipleColumnFamilyValues(List<String> keyList) {
        List<byte[]> keyByteList = keyList.stream().map(String::getBytes).collect(Collectors.toList());
        List<Object> resultValueList = new ArrayList<>();
        LOGGER.info("---[DB] Operation ColFam GET WITH MULTIPLE KEYS. Key list: {}.", keyList);
        try {
            List<byte[]> byteValueList = rocksDB.multiGetAsList(columnFamilyHandleList, keyByteList);
            if (!byteValueList.isEmpty()) {
                resultValueList = byteValueList.stream().map(SerializationUtils::deserialize).collect(Collectors.toList());
            }
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] ColFam MULTIPLE GET error. Cause: [{}], message [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        LOGGER.info("---[DB] ColFam values for keys are: {}", resultValueList);
        return resultValueList.isEmpty() ? Optional.empty() : Optional.of(resultValueList);
    }

    public boolean deleteMultipleColumnFamilyValues(String columnFamilyHandleName, String beginKey, String endKey) {
        ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandleByName(columnFamilyHandleName.getBytes());
        try {
            rocksDB.deleteRange(columnFamilyHandle, beginKey.getBytes(), endKey.getBytes());
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] ColFam MULTIPLE DELETE error. Cause: [{}] , message: [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return true;
    }

    public void mergeTest(String key, Object value) {
        try {
            LOGGER.info("---MERGE TEST---");
            Optional<List<Object>> valueList = find(key);
            if (valueList.isPresent()) {
                LOGGER.info("---Values before: {} ", valueList);
            }
            rocksDB.merge(key.getBytes(), SerializationUtils.serialize(value));
            LOGGER.info("---MERGED VALUES---");
            valueList = find(key);
            if (valueList.isPresent()) {
                LOGGER.info("---Values after: {}", valueList);
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean addValue(String key, Object value) {
        List<Object> valueList = new LinkedList<>();
        Optional<List<Object>> findResult = find(key);
        if (findResult.isPresent()) {
            valueList = findResult.get();
        }
        boolean present = valueList.stream().anyMatch(i -> i.equals(value));
        if (!present) valueList.add(value);
        try {
            rocksDB.put(key.getBytes(), SerializationUtils.serialize(valueList));
            LOGGER.info("---[DB] Values: [{}].", valueList);
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] ADD error. Cause: [{}] , message: [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return true;
    }
    private ColumnFamilyHandle getColumnFamilyHandleByName(byte[] name) {
        return columnFamilyHandleList
                .stream()
                .filter(
                        handle -> {
                            try {
                                return Arrays.equals(handle.getName(), name);
                            } catch (Exception ex) {
                                throw new RuntimeException(ex);
                            }
                        })
                .findAny()
                .orElse(null);
    }
}
