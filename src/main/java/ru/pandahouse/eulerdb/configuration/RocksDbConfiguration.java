package ru.pandahouse.eulerdb.configuration;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


@Configuration
public class RocksDbConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbConfiguration.class);
    private static final String DB_PATH = "/home/alexunderit/rocksdb";
    private RocksDB db;
    private DBOptions dbOptions;

    /*private Options options;*/
    private ColumnFamilyOptions cfOpts;
    private List<ColumnFamilyHandle> columnFamilyHandleList;

    @Bean
    public RocksDB rocksDb(List<ColumnFamilyDescriptor> cfDescriptors,
                           List<ColumnFamilyHandle> columnFamilyHandleList,
                           DBOptions dbOptions)
                           /*Options options)*/
    throws RocksDBException {
        RocksDB.loadLibrary();
        File dbDir = new File(DB_PATH);
        long startMillis = System.currentTimeMillis();
        try {
            db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(),cfDescriptors,columnFamilyHandleList);
        } catch (RocksDBException e) {
            LOGGER.error(e.getMessage());
        }
        long openMillis = System.currentTimeMillis();

        LOGGER.info("---[STARTED DB IN {} MS]---", openMillis - startMillis);

        return db;
    }

    @Bean
    public List<ColumnFamilyDescriptor> cfDescriptors(ColumnFamilyOptions columnFamilyOptions) {
        return Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                new ColumnFamilyDescriptor("metadata".getBytes(StandardCharsets.UTF_8), columnFamilyOptions),
                new ColumnFamilyDescriptor("index_to_hash".getBytes(StandardCharsets.UTF_8), columnFamilyOptions)
        );
    }

    @Bean
    public DBOptions dbOptions() {
        //FIXME: суть в чем: есть оператор merge(), но мы его не можем использовать,
        // если используем колоночные семейства, потому что метод setMergeOperator() в
        // в DBOptions отсутствует, а без DBOptions не запустить бд с колоночными семействами.
        /*Options options1 = new Options();
        options1.setMergeOperatorName("StringMergeOperator");
        StringAppendOperator stringAppendOperator = new StringAppendOperator();
        options1.setMergeOperator(stringAppendOperator);*/
        dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);
        return dbOptions;
    }

    @Bean
    public ColumnFamilyOptions columnFamilyOptions() {
        cfOpts = new ColumnFamilyOptions()
                .optimizeUniversalStyleCompaction()
                /*.setMergeOperator(какой-то мердж оператор)*/;
        return cfOpts;
    }

    @Bean
    @Qualifier("columnFamilies")
    public List<ColumnFamilyHandle> columnFamilyConfig() {
        columnFamilyHandleList = new ArrayList<>();
        return columnFamilyHandleList;
    }

    /*@Bean
    public Options options() {
        return options = new Options()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMergeOperator(new StringAppendOperator(", "));

    }*/

    @PreDestroy
    public void closeConnections() {
        for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
            columnFamilyHandle.close();
        }
        dbOptions.close();
        cfOpts.close();
        LOGGER.info("----[CLOSE DATABASE]----");
        db.close();
    }
}
