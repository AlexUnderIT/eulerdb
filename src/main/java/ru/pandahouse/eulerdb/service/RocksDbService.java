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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class RocksDbService implements KVRepository<byte[], byte[]> {

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
    public boolean save(byte[] key, byte[] value) {
        LOGGER.info("---[DB] Operation PUT. Value: [{}], Key: [{}]",
                new String(value), new String(key));
        try{
            rocksDB.put(key, value);
        } catch(RocksDBException e){
            LOGGER.error("---[ERROR] PUT error. Cause: {} , message: {}", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public Optional<byte[]> find(byte[] key) {
        byte[] value = new byte[0];
        LOGGER.info("---[DB] Operation GET. Key: [{}].", new String(key));
        try {
            byte[] bytes = rocksDB.get(key);
            if (bytes != null){
                value = bytes;
                LOGGER.info("---[DB] GET value [{}] with key [{}].", new String(value),new String(key));
            }
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] GET error. Cause: {} , message: [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return value.length == 0 ? Optional.of(value) : Optional.empty();
    }

    @Override
    public boolean delete(byte[] key) {
        LOGGER.info("---[DB] Operation DELETE. Key: [{}].", key);
        try {
            rocksDB.delete(key);
            LOGGER.info("---[DB] DELETE value with key [{}].", new String(key));
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] DELETE error. Cause: [{}] , message: [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return true;
    }
    /*@Override
    public boolean add(byte[] key, byte[] value) {
        LOGGER.info("---[DB] Operation ADD. Key: [{}], value [{}]", new String(key), SerializationUtils.deserialize(value));
        return addValue(key, value);
    }*/

    public Optional<List<byte[]>> findMultipleValues(List<byte[]> keyList) {
        List<byte[]> valueList = new ArrayList<>();
        LOGGER.info("---[DB] Operation GET WITH MULTIPLE KEYS. Key list: {}.", keyList);
        try {
            List<byte[]> valueByteList = rocksDB.multiGetAsList(keyList);
            if (!valueByteList.isEmpty()) {
                valueList = valueByteList;
            }
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] GET WITH MULTIPLE KEYS error. Cause: {} , message: {}---", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        LOGGER.info("Values for this list: {}", valueList);
        return valueList.isEmpty() ? Optional.empty() : Optional.of(valueList);
    }

    //Удаляет в диапазоне [beginKey, endKey), не включая значения endKey
    public boolean deleteMultipleKeys(byte[] beginKey, byte[] endKey) {
        LOGGER.info("---[DB] Operation DELETE WITH MULTIPLE KEYS. Begin key: [{}], end key (not included)   : [{}]", beginKey, endKey);
        try {
            rocksDB.deleteRange(beginKey, endKey);
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] DELETE WITH MULTIPLE KEYS error. Cause: {} , message: {}.", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return true;
    }

    public boolean saveColumnFamily(String columnFamilyHandleName, byte[] key, byte[] value) {
        ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandleByName(columnFamilyHandleName.getBytes(StandardCharsets.UTF_8));
        if (columnFamilyHandle == null) {
            LOGGER.error("---[ERROR] ColumnFamilyHandle with name [{}] didn't find.", columnFamilyHandleName);
            return false;
        }
        try {
            rocksDB.put(columnFamilyHandle, key, value);
            LOGGER.info("---[DB] PUT value [{}] with key [{}] in ColumnFamily [{}].", SerializationUtils.deserialize(value), new String(key), columnFamilyHandleName);
            return true;
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] ColFam PUT ERROR. Cause: [{}], message: [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public Optional<byte[]> findColumnFamilyValue(String columnFamilyHandleName, byte[] key) {
        ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandleByName(columnFamilyHandleName.getBytes(StandardCharsets.UTF_8));
        byte[] resultByte = null;
        Object result = new Object();
        try {
            resultByte = rocksDB.get(columnFamilyHandle, key);
            if (resultByte!=null) {
                LOGGER.info("---[DB] GET value [{}] with key [{}] from ColumnFamily [{}].", SerializationUtils.deserialize(resultByte), new String(key), columnFamilyHandleName);
            } else {
                LOGGER.info("---[DB] Value with such key didn't find.---");
            }
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] ColFam GET ERROR. Cause: [{}], message: [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return resultByte == null ? Optional.empty() : Optional.of(resultByte);
    }

    public boolean deleteColumnFamilyValue(String columnFamilyHandleName, byte[] key) {
        ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandleByName(columnFamilyHandleName.getBytes());
        try {
            rocksDB.delete(columnFamilyHandle, key);
            LOGGER.info("---[DB] DELETE value with key [{}] from ColumnFamily [{}].", key, columnFamilyHandleName);
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] ColFam DELETE error. Cause: [{}], message: [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e    );
        }
        return true;
    }

    public Optional<List<byte[]>> findMultipleColumnFamilyValues(List<byte[]> keyList) {
        List<byte[]> resultValueList = new ArrayList<>();
        LOGGER.info("---[DB] Operation ColFam GET WITH MULTIPLE KEYS. Key list: {}.", keyList);
        try {
            List<byte[]> byteValueList = rocksDB.multiGetAsList(columnFamilyHandleList, keyList);
            if (!byteValueList.isEmpty()) {
                LOGGER.info("---[DB] ColFam findMulti values [{}] in ColumnFamilies", byteValueList.stream().map(SerializationUtils::deserialize).collect(Collectors.toList()));
                resultValueList = byteValueList;
            }
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] ColFam MULTIPLE GET error. Cause: [{}], message [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        LOGGER.info("---[DB] ColFam values for keys are: {}", resultValueList);
        return resultValueList.isEmpty() ? Optional.empty() : Optional.of(resultValueList);
    }

    public boolean deleteMultipleColumnFamilyValues(String columnFamilyHandleName, byte[] beginKey, byte[] endKey) {
        ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandleByName(columnFamilyHandleName.getBytes());
        try {
            rocksDB.deleteRange(columnFamilyHandle, beginKey, endKey);
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] ColFam MULTIPLE DELETE error. Cause: [{}] , message: [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return true;
    }

    public void mergeTest(byte[] key, byte[] value) {
        try {
            LOGGER.info("---MERGE TEST---");
            Optional<byte[]> valueOpt = find(key);
            if (valueOpt.isPresent()) {
                LOGGER.info("---Values before: {} ", SerializationUtils.deserialize(valueOpt.get()));
            }
            rocksDB.merge(key, value);
            LOGGER.info("---MERGED VALUES---");
            valueOpt = find(key);
            if (valueOpt.isPresent()) {
                LOGGER.info("---Values after: {}", valueOpt);
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    /*private boolean addValue(byte[] key, byte[] value) {
        List<byte[]> valueList = new ArrayList<>();
        Optional<byte[]> findResult = find(key);
        if (findResult.isPresent()) {
            valueList = findResult.get();
        }
        boolean present = valueList.stream().anyMatch(i -> Arrays.equals(i, value));
        if (!present) valueList.add(value);
        try {
            rocksDB.put(key, SerializationUtils.serialize(valueList));
            LOGGER.info("---[DB] Values: {}.", valueList.stream().map(SerializationUtils::deserialize).collect(Collectors.toList()));
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] ADD error. Cause: [{}] , message: [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return true;
    }*/
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
