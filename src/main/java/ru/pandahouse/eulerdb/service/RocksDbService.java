package ru.pandahouse.eulerdb.service;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;
import ru.pandahouse.eulerdb.exceptions.ColumnFamilyNotFoundException;
import ru.pandahouse.eulerdb.repository.KVRepository;

import java.util.*;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

@Service
public class RocksDbService implements KVRepository<byte[], byte[]> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbService.class);

    private final RocksDB rocksDB;
    private final List<ColumnFamilyHandle> columnFamilyHandleList;
    private final DBOptions dbOptions;
    private final SstFileManager sstFileManager;

    @Autowired
    public RocksDbService(RocksDB rocksDB, List<ColumnFamilyHandle> columnFamilyHandleList, DBOptions dbOptions, SstFileManager sstFileManager) {
        this.rocksDB = rocksDB;
        this.columnFamilyHandleList = columnFamilyHandleList;
        this.dbOptions = dbOptions;
        this.sstFileManager = sstFileManager;
    }

    @Override
    public boolean save(byte[] key, byte[] value) {
        LOGGER.info("---[DB] Operation PUT. Value: [{}], Key: [{}]", new String(value), new String(key));
        try {
            rocksDB.put(key, value);
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] PUT error. Cause: {} , message: {}", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public Optional<byte[]> find(byte[] key) {
        byte[] value = new byte[0];
        //LOGGER.info("---[DB] Operation GET. Key: [{}].", new String(key));
        try {
            byte[] bytes = rocksDB.get(key);
            if (bytes != null) {
                value = bytes;
                //LOGGER.info("---[DB] GET value [{}] with key [{}].", new String(value), new String(key));
            }
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] GET error. Cause: {} , message: [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        //if (value.length == 0) LOGGER.info("---[WARN] Didn't find any value with such key...");
        return value.length != 0 ? Optional.of(value) : Optional.empty();
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
    @Override
    public boolean add(byte[] key, byte[] value) {
        try {
            LOGGER.info("---[DB] Operation ADD. Key: [{}], value: [{}]", new String(key), new String(value));
            byte[] resultByte = rocksDB.get(key);
            if(resultByte != null){
                String[] resultString = new String(rocksDB.get(key)).split(", ");
                Optional<String> isNotNew = Arrays.stream(resultString).filter(i -> i.equals(new String(value))).findAny();
                if (isNotNew.isPresent()) {
                    LOGGER.warn("---[WARN] Such value is on the database. Rollback operation...");
                    return false;
                }
            }
            else {
                LOGGER.warn("---[WARN] There is no values with such key. Making SAVE instead of ADD.");
                this.save(key, value);
                return true;
            }
            rocksDB.merge(key, value);
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] ADD operation ERROR. Cause: {}, message: {}", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return true;
    }

    public Optional<List<byte[]>> findMultipleValues(List<byte[]> keyList) {
        List<byte[]> valueList = new ArrayList<>();
        List<String> keyStringList = keyList.stream().map(String::new).collect(Collectors.toList());
        LOGGER.info("---[DB] Operation GET WITH MULTIPLE KEYS. Key list: {}.", keyStringList);
        try {
            List<byte[]> valueByteList = rocksDB.multiGetAsList(keyList);
            if (!valueByteList.isEmpty()) {
                valueList = valueByteList;
            }
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] GET WITH MULTIPLE KEYS error. Cause: {} , message: {}---", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        List<String> stringValues = valueList.stream().map(String::new).collect(Collectors.toList());
        LOGGER.info("Values for this list: {}", stringValues);
        return valueList.isEmpty() ? Optional.empty() : Optional.of(valueList);
    }

    //Удаляет в диапазоне [beginKey, endKey), не включая значения endKey
    public boolean deleteMultipleKeys(byte[] beginKey, byte[] endKey) {
        LOGGER.info("---[DB] Operation DELETE WITH MULTIPLE KEYS. Begin key: [{}], end key (not included)   : [{}]", new String(beginKey), new String(endKey));
        try {
            rocksDB.deleteRange(beginKey, endKey);
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] DELETE WITH MULTIPLE KEYS error. Cause: {} , message: {}.", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return true;
    }

    public boolean saveColumnFamily(String columnFamilyHandleName, byte[] key, byte[] value) {
        ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandleByName(columnFamilyHandleName.getBytes(UTF_8));
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
        ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandleByName(columnFamilyHandleName.getBytes(UTF_8));
        byte[] resultByte;
        try {
            resultByte = rocksDB.get(columnFamilyHandle, key);
            if (resultByte != null) {
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
            LOGGER.info("---[DB] DELETE value with key [{}] from ColumnFamily [{}].", new String(key), columnFamilyHandleName);
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] ColFam DELETE error. Cause: [{}], message: [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return true;
    }

    public boolean addColumnFamilyValue(String columnFamilyHandleName, byte[] key, byte[] value) {
        ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandleByName(columnFamilyHandleName.getBytes());
        try {
            LOGGER.info("---[DB] Add value with key [{}] to ColumnFamily [{}].", new String(key), columnFamilyHandleName);
            rocksDB.merge(columnFamilyHandle, key, value);
        } catch (RocksDBException e) {
            LOGGER.error("---[ERROR] ColFam ADD error. Cause: {}, message: {}.", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return true;
    }

    //Чтобы работал надо подавать ключи в том же порядке, что и список колонок,
    //иначе не работает, т.е. первый ключ должен быть в первой по списку колонке.
    public Optional<List<byte[]>> findMultipleColumnFamilyValues(List<byte[]> keyList) {
        List<byte[]> resultValueList = new ArrayList<>();
        List<String> keyStringList = keyList.stream().map(String::new).collect(Collectors.toList());
        LOGGER.info("---[DB] Operation ColFam GET WITH MULTIPLE KEYS. Key list: {}.", keyStringList);
        try {
            List<byte[]> byteValueList = rocksDB.multiGetAsList(columnFamilyHandleList, keyList);
            if (!byteValueList.isEmpty()) {
                LOGGER.info("---[DB] ColFam findMulti values [{}] in ColumnFamilies", byteValueList);
                resultValueList = byteValueList;
            }
        } catch (RocksDBException e) {
                LOGGER.error("---[ERROR] ColFam MULTIPLE GET error. Cause: [{}], message [{}].", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        List<String> valueStringList = resultValueList.stream()
                .filter(Objects::nonNull)
                .map(String::new)
                .collect(Collectors.toList());
        LOGGER.info("---[DB] ColFam values for keys are: {}", valueStringList);
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
                .orElseThrow(ColumnFamilyNotFoundException::new);
    }
    public void getStatistic(){
        try {
        LOGGER.info("----DATABASE STATISTIC----");

        Snapshot currentSnapshot = rocksDB.getSnapshot();
        LOGGER.info("-[STATS] Snapshot sequence num: {}", currentSnapshot.getSequenceNumber());
        rocksDB.releaseSnapshot(currentSnapshot);

        LOGGER.info("-[STATS] Hits to memtable during this session: {}.", dbOptions.statistics().getTickerCount(TickerType.MEMTABLE_HIT));
        LOGGER.info("-[STATS] Number of seek to db and returns: {}.", dbOptions.statistics().getTickerCount(TickerType.NUMBER_DB_SEEK_FOUND));
        LOGGER.info("-[STATS] WAL written by {} bytes.", dbOptions.statistics().getTickerCount(TickerType.WRITE_WITH_WAL));

        LOGGER.info("-[STATS] Total disk usage by DB: {} bytes, or {} mb.", sstFileManager.getTotalSize(), ((float)(sstFileManager.getTotalSize())) / (1024 * 1024));
        Map<String, Long> sstFileMap =  sstFileManager.getTrackedFiles();
        LOGGER.info("-[STATS] Tracked SST files are: \n{}.", sstFileMap.toString());

        //Что каждая строчка значит: https://github.com/facebook/rocksdb/wiki/Compaction-Stats-and-DB-Status
        LOGGER.info("-[STATS] RocksDB stats: \n{}", rocksDB.getProperty("rocksdb.stats"));
        } catch (RocksDBException e){
            LOGGER.error("--[ERROR] Cause: {}, message: {}.", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
