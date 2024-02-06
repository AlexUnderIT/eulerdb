package ru.pandahouse.eulerdb.traversal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.pandahouse.eulerdb.service.RocksDbService;

import java.util.*;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
/*
Class, that implements BFS algorithm
*/
@Component
public class BreadthFirstSearchAlgo {
    private static final Logger LOGGER = LoggerFactory.getLogger(BreadthFirstSearchAlgo.class);
    private final RocksDbService rocksDbService;

    @Autowired
    public BreadthFirstSearchAlgo(RocksDbService rocksDbService) {
        this.rocksDbService = rocksDbService;
    }

    public void allGraphBfsTraversal(String startNode) {
        long startMillis = System.currentTimeMillis();
        List<String> neighbourNodes = Collections.emptyList();
        Queue<String> queue = new LinkedList<>();
        LOGGER.info("---START GRAPH BFS TRAVERSAL---");
        List<String> visited = new ArrayList<>();
        int COUNTER = 1;

        queue.add(startNode);
        visited.add(startNode);
        while(!queue.isEmpty()){
            String node = queue.poll();
            LOGGER.info("Node: [{}] num [{}]", node, COUNTER);
            COUNTER++;
            Optional<byte[]> neighboursByteOpt = rocksDbService.find(node.getBytes(UTF_8));
            if(neighboursByteOpt.isPresent()){
                neighbourNodes = getNeighboursWithoutPrefix(neighboursByteOpt.get());
            } else{
                LOGGER.error("---[ERROR] There are no such node in graph. Rollback");
                //throw new RuntimeException("No such node in graph");
            }
            if(!neighbourNodes.isEmpty()){
                for(String nodes: neighbourNodes){
                    if(!visited.contains(nodes)){
                        queue.add(nodes);
                        visited.add(nodes);
                    }
                }
            }
        }
        long endMillis = System.currentTimeMillis();
        LOGGER.info("---END GRAPH BFS TRAVERSAL---");
        LOGGER.info("TOTAL TIME TO TRAVERSE: {}", endMillis-startMillis);
    }
    private List<String> getNeighboursWithoutPrefix(byte[] neighboursByte){
        String[] parseToString = new String(neighboursByte).split(", ");
        return Arrays
                .stream(parseToString)
                .map(i -> i.substring(1)).collect(Collectors.toList());
    }

}
