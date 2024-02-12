package ru.pandahouse.eulerdb.traversal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.pandahouse.eulerdb.service.RocksDbService;

import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

@Component
public class DepthFirstSearchAlgo extends TraversalAlgorithm {
    private static final Logger LOGGER = LoggerFactory.getLogger(DepthFirstSearchAlgo.class);
    private final RocksDbService rocksDbService;

    @Autowired
    public DepthFirstSearchAlgo(RocksDbService rocksDbService) {
        this.rocksDbService = rocksDbService;
    }

    public void allGraphDfsTraversal(String startNode) {
        COUNTER = 0;
        Set<String> visited = new HashSet<>();
        LOGGER.info("---START GRAPH DFS TRAVERSAL---");

        long startTime = System.currentTimeMillis();
        //dfs(startNode, visited);
        dfs_without_recursion(startNode, visited);
        long endTime = System.currentTimeMillis();

        LOGGER.info("---END GRAPH DFS TRAVERSAL---");
        LOGGER.info("TOTAL TIME TO DFS TRAVERSE: {} ms, {} seconds.", endTime - startTime, (float) (endTime - startTime) / 1000);
        LOGGER.info("Total count of traversed nodes: {}", COUNTER);
        LOGGER.info("Total time to DB queries: {} ms", timeToDbQuery);
        LOGGER.info("Time to DFS without DB queries: {} ms", (endTime - startTime) - timeToDbQuery);
        timeToDbQuery = 0;
        COUNTER = 0;
    }

    public void dfs(String node, Set<String> visited) {
        List<String> neighbourNodes = new ArrayList<>();
        COUNTER++;
        visited.add(node);

        long startQuery = System.currentTimeMillis();
        Optional<byte[]> neighboursByteOpt = rocksDbService.find(node.getBytes(UTF_8));
        long endQuery = System.currentTimeMillis();
        timeToDbQuery += (endQuery - startQuery);

        if (neighboursByteOpt.isPresent()) {
            neighbourNodes = getNeighboursWithoutPrefix(neighboursByteOpt.get());
        }
        if (!neighbourNodes.isEmpty()) {
            for (String nodes : neighbourNodes) {
                if (!visited.contains(nodes)) {
                    dfs(nodes, visited);
                }
            }
        }
    }

    public void dfs_without_recursion(String node, Set<String> visited) {
        Deque<String> stack = new ArrayDeque<>();
        List<String> neighbourNodes = new ArrayList<>();

        stack.push(node);
        while(!stack.isEmpty()){
            String currentNode = stack.pop();

            long startQuery = System.currentTimeMillis();
            Optional<byte[]> neighboursByteOpt = rocksDbService.find(currentNode.getBytes(UTF_8));
            long endQuery = System.currentTimeMillis();
            timeToDbQuery += (endQuery - startQuery);

            if(neighboursByteOpt.isPresent()){
                neighbourNodes = getNeighboursWithoutPrefix(neighboursByteOpt.get());
            }
            if(!visited.contains(currentNode)){
                visited.add(currentNode);
                COUNTER++;
                for(String nodes: neighbourNodes){
                    if(!visited.contains(nodes)){
                        stack.push(nodes);
                    }
                }
            }
        }
    }

}
