package ru.pandahouse.eulerdb.controller;

import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.internal.storage.commitgraph.CommitGraph;
import org.eclipse.jgit.internal.storage.file.FileCommitGraph;
import org.eclipse.jgit.internal.storage.file.ObjectDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.SerializationUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import ru.pandahouse.eulerdb.configuration.GFParse;
import ru.pandahouse.eulerdb.configuration.GraphParseWithoutDirection;
import ru.pandahouse.eulerdb.configuration.JGitConfig;
import ru.pandahouse.eulerdb.service.RocksDbService;
import ru.pandahouse.eulerdb.traversal.BreadthFirstSearchAlgo;
import ru.pandahouse.eulerdb.traversal.DepthFirstSearchAlgo;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

@RestController
public class IndexController {

    Logger LOGGER = LoggerFactory.getLogger(IndexController.class);
    private final RocksDbService rocksDbService;
    private final GFParse gfParse;
    private final GraphParseWithoutDirection graphParseWithoutDirection;
    private final BreadthFirstSearchAlgo breadthFirstSearchAlgo;
    private final DepthFirstSearchAlgo depthFirstSearchAlgo;
    private final JGitConfig jGitConfig;


    @Autowired
    public IndexController(
            RocksDbService rocksDbService,
            GFParse gfParse, GraphParseWithoutDirection graphParseWithoutDirection,
            BreadthFirstSearchAlgo breadthFirstSearchAlgo,
            DepthFirstSearchAlgo depthFirstSearchAlgo, JGitConfig jGitConfig
    ) {
        this.rocksDbService = rocksDbService;
        this.gfParse = gfParse;
        this.graphParseWithoutDirection = graphParseWithoutDirection;
        this.breadthFirstSearchAlgo = breadthFirstSearchAlgo;
        this.depthFirstSearchAlgo = depthFirstSearchAlgo;
        this.jGitConfig = jGitConfig;
    }

    @GetMapping("/put/{key}/{value}")
    public void putSomething(@PathVariable("key") String key,
                             @PathVariable("value") String value){
        rocksDbService.save(key.getBytes(), value.getBytes());
    }
    @GetMapping("/find/{key}")
    public void getByKey(@PathVariable("key") String key){
        rocksDbService.find(key.getBytes());
    }
    @GetMapping("/delete/{key}")
    public void deleteByKey(@PathVariable("key") String key){
        rocksDbService.delete(key.getBytes());
    }
    @GetMapping("/add/{key}/{value}")
    public void mergeTest(@PathVariable("key") String key,
                          @PathVariable("value") String value){
        rocksDbService.add(key.getBytes(),value.getBytes());
    }
    @GetMapping("/putCol/{name}/{key}/{value}")
    public void putColumnFamilyData(@PathVariable("name") String name,
                          @PathVariable("key") String key,
                          @PathVariable("value") String value){
        rocksDbService.saveColumnFamily(name, key.getBytes(StandardCharsets.UTF_8), value.getBytes());
    }
    @GetMapping("/findCol/{name}/{key}")
    public void getColumnFamilyData(@PathVariable("name") String name,
                                    @PathVariable("key") String key){
        rocksDbService.findColumnFamilyValue(name, key.getBytes());
    }
    @GetMapping("/delCol/{name}/{key}")
    public void delColumnFamilyData(@PathVariable("name") String name,
                                    @PathVariable("key") String key){
        rocksDbService.deleteColumnFamilyValue(name, key.getBytes());
    }
    @GetMapping("/addCol/{name}/{key}/{value}")
    public void addColumnFamilyData(@PathVariable("name") String name,
                                    @PathVariable("key") String key,
                                    @PathVariable("value") String value){
        rocksDbService.addColumnFamilyValue(name,key.getBytes(),value.getBytes());
    }
    @GetMapping("/col/find")
    public void multipleColFindTest(){
        rocksDbService.findMultipleColumnFamilyValues(List.of("1234".getBytes(),
                "2345".getBytes(),"3456".getBytes()));
    }
    @GetMapping("col/del/{name}")
    public void multipleColDelTest(@PathVariable("name") String name){
        rocksDbService.deleteMultipleColumnFamilyValues(name,"1111".getBytes(),"2222".getBytes());
    }
    @GetMapping("/getMulti")
    public void getMultiByKeyList(){
        rocksDbService.findMultipleValues(List.of("1111".getBytes(),"2222".getBytes(),"3333".getBytes()));
    }
    @GetMapping("/delMulti/{key_1}/{key_2}")
    public void delMultiByKeyList(@PathVariable("key_1") String firstKey,
                                  @PathVariable("key_2") String secondKey){
        rocksDbService.deleteMultipleKeys(firstKey.getBytes(), secondKey.getBytes());
    }
    @GetMapping("/readFile/{name}")
    public void readFile(@PathVariable("name") String fileName){
        gfParse.parseFile(fileName + ".txt");
    }
    @GetMapping("/readFileWD")
    public void readFileWD(){
        graphParseWithoutDirection
                .parseFileWithoutDirection(GraphParseWithoutDirection.PATH_TO_FILE);
    }
    @GetMapping("/bfs/{key}")
    public void bfsTest(@PathVariable("key") String startKey){
        breadthFirstSearchAlgo.allGraphBfsTraversal(startKey);
    }
    @GetMapping("/dfs/{key}")
    public void dfsTest(@PathVariable("key") String startKey){
        depthFirstSearchAlgo.allGraphDfsTraversal(startKey);
    }
    @GetMapping("stats")
    public void getStats(){
        rocksDbService.getStatistic();
    }
    /*@GetMapping("graph/{hash}")
    public void commitGraphFileTest(@PathVariable("hash") String commitHash){
        try {
            LOGGER.info("----COMMIT-GRAPH FILE TEST START");
            CommitGraph commitGraph = jGitConfig.getCommitGraphFile();

            File cgFile = new File(commitGraph);
            ObjectDirectory objectDirectory = new

            rocksDbService.save("1".getBytes(), SerializationUtils.serialize(commitGraph));
            JGitConfig.getComitGraphFileInfo(commitGraph,commitHash);

            LOGGER.info("----COMMIT-GRAPH FILE TEST MIDDLE");
            Optional<byte[]> cgOpt = rocksDbService.find("1".getBytes());
            if(cgOpt.isPresent()){
                CommitGraph commitGraph1 = (CommitGraph) SerializationUtils.deserialize(cgOpt.get());
            }
            JGitConfig.getComitGraphFileInfo(commitGraph,commitHash);

            LOGGER.info("----COMMIT-GRAPH FILE TEST END");
        } catch (IOException | GitAPIException e) {
            throw new RuntimeException(e);
        }
    }*/
}
