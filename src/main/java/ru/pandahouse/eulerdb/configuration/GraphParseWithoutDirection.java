package ru.pandahouse.eulerdb.configuration;

import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;
import ru.pandahouse.eulerdb.graph.Metadata;
import ru.pandahouse.eulerdb.service.RocksDbService;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;
/*
*  Класс, в котором файл парсится без добавления префикса
* отвечающего за направление
*/
@Component
public class GraphParseWithoutDirection {
    private final Logger LOGGER = LoggerFactory.getLogger(GraphParseWithoutDirection.class);
    public static final String PATH_TO_FILE = "/home/alexunderit/IdeaProjects/RepoProjects/eulerdb/test_all.txt";
    private final String METADATA = "metadata";

    private final RocksDbService rocksDbService;
    private final RocksDB rocksDB;

    @Autowired
    public GraphParseWithoutDirection(RocksDbService rocksDbService, RocksDB rocksDb) {
        this.rocksDbService = rocksDbService;
        this.rocksDB = rocksDb;
    }

    public void parseFileWithoutDirection(String pathToFile){
        long startParse = System.currentTimeMillis();
        try (BufferedReader bufferReader = new BufferedReader(new FileReader(pathToFile))){
            String line;
            while ((line = bufferReader.readLine()) != null) {
                boolean notOneParent = false;
                Metadata metadata = new Metadata();

                String[] stringData = line.split("~");

                String currentNodeHash = stringData[0];
                String shortHash = stringData[1];
                String authorId = stringData[2];
                String authorEmail = stringData[3];
                String title = stringData[4];
                String parentsHash = stringData[5];

                // [0] - hashCode
                // [5] - parentHashCode
                metadata.setShortHash(shortHash);
                metadata.setAuthorId(authorId);
                metadata.setAuthorEmail(authorEmail);
                metadata.setTitle(title);
                //Если несколько родителей - помечаем флаг.
                if (parentsHash.contains(" ")) notOneParent = true;

                //Если
                if (rocksDB.keyExists(currentNodeHash.getBytes(UTF_8))) {
                    if (notOneParent) {
                        addMultiParentsWND(currentNodeHash,parentsHash.split(" "));
                    } else{
                        addParentWND(currentNodeHash,parentsHash);
                    }
                } else {
                    if(notOneParent){
                        saveMultiParentWND(currentNodeHash,parentsHash.split(" "));
                    } else{
                        saveParentWND(currentNodeHash,parentsHash);
                    }
                }
                if (!parentsHash.contains("-")) {
                    if(notOneParent){
                        addChildToMultiParentsWND(parentsHash.split(" "), currentNodeHash);
                    } else{
                        addChildToOneParentWND(parentsHash, currentNodeHash);
                    }
                }
                //Добавляем метадату в семейство колонок
                addMetadataToColumnFamilyWND(metadata,currentNodeHash);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long endParse = System.currentTimeMillis();
        LOGGER.info("Time to parse file: {} ms", endParse - startParse);
    }
    private void addMultiParentsWND(String currentNode, String[] parentsNodes) {
        Arrays
                .stream(parentsNodes)
                .forEach(i -> rocksDbService.add(currentNode.getBytes(UTF_8), i.getBytes(UTF_8)));
    }

    private void addParentWND(String currentNode, String parentNode) {
        rocksDbService.add(currentNode.getBytes(UTF_8), parentNode.getBytes(UTF_8));
    }

    private void addChildToOneParentWND(String parentNode, String currentNode) {
        rocksDbService.add(parentNode.getBytes(UTF_8), currentNode.getBytes(UTF_8));
    }
    private void addChildToMultiParentsWND(String[] parentsNodes, String currentNode){
        Arrays.stream(parentsNodes).forEach(i -> rocksDbService.add(i.getBytes(UTF_8), currentNode.getBytes(UTF_8)));
    }
    private void saveMultiParentWND(String currentNode, String[] parentsNode){
        rocksDbService.save(currentNode.getBytes(UTF_8),parentsNode[0].getBytes(UTF_8));
        Arrays.stream(parentsNode)
                .skip(1)
                .forEach(i ->
                        rocksDbService.add(currentNode.getBytes(UTF_8),
                                i.getBytes(UTF_8))
                );
    }
    private void saveParentWND(String currentNode, String parentNode){
        rocksDbService.save(currentNode.getBytes(UTF_8), parentNode.getBytes(UTF_8));
    }
    private void addMetadataToColumnFamilyWND(Metadata metadata, String currentHash){
        rocksDbService.saveColumnFamily(METADATA, currentHash.getBytes(UTF_8), SerializationUtils.serialize(metadata));
    }


}
