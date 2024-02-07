package ru.pandahouse.eulerdb.traversal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.pandahouse.eulerdb.service.RocksDbService;

import java.util.*;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

@Component
public class DepthFirstSearchAlgo extends TraversalAlgorithm{
    private static final Logger LOGGER = LoggerFactory.getLogger(DepthFirstSearchAlgo.class);
    private final RocksDbService rocksDbService;

    @Autowired
    public DepthFirstSearchAlgo(RocksDbService rocksDbService) {
        this.rocksDbService = rocksDbService;
    }
    public void allGraphDfsTraversal(String startNode){
        COUNTER = 0;
        List<String> visited = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        LOGGER.info("---START GRAPH DFS TRAVERSAL---");

        dfs(startNode, visited);

        long endTime = System.currentTimeMillis();
        LOGGER.info("---END GRAPH DFS TRAVERSAL---");
        LOGGER.info("TOTAL TIME TO DFS TRAVERSE: {} ms, {} seconds.", endTime - startTime, (float)(endTime - startTime)/1000 );
        LOGGER.info("Total count of traversed nodes: {}", COUNTER);
    }
    public void dfs(String node, List<String> visited){
        List<String> neighbourNodes = Collections.emptyList();

        //LOGGER.info("Node: [{}] num [{}] ", node,COUNTER);
        COUNTER++;
        visited.add(node);

        Optional<byte[]> neighboursByteOpt = rocksDbService.find(node.getBytes(UTF_8));
        if(neighboursByteOpt.isPresent()){
            neighbourNodes = getNeighboursWithoutPrefix(neighboursByteOpt.get());
        } else{
            LOGGER.error("---[ERROR] There are no node [{}] in graph. Rollback", node);
            //throw new RuntimeException("No such node in graph");
        }if(!neighbourNodes.isEmpty()){
            for(String nodes: neighbourNodes){
                if(!visited.contains(nodes)){
                    dfs(nodes, visited);
                }
            }
        }
    }
}
