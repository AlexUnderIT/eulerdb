package ru.pandahouse.eulerdb.traversal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.pandahouse.eulerdb.service.RocksDbService;

import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;
/*
Class, that implements BFS algorithm
*/
@Component
public class BreadthFirstSearchAlgo extends TraversalAlgorithm{
    private static final Logger LOGGER = LoggerFactory.getLogger(BreadthFirstSearchAlgo.class);
    private final RocksDbService rocksDbService;

    @Autowired
    public BreadthFirstSearchAlgo(RocksDbService rocksDbService) {
        this.rocksDbService = rocksDbService;
    }

    public void allGraphBfsTraversal(String startNode) {
        List<String> neighbourNodes = new LinkedList<>();
        Queue<String> queue = new ArrayDeque<>();
        COUNTER = 0;
        Set<String> visited = new HashSet<>();

        LOGGER.info("---START GRAPH BFS TRAVERSAL---");
        long startMillis = System.currentTimeMillis();

        queue.add(startNode);
        visited.add(startNode);
        while(!queue.isEmpty()){
            String node = queue.poll();
            COUNTER++;
            long startQuery = System.currentTimeMillis();
            Optional<byte[]> neighboursByteOpt = rocksDbService.find(node.getBytes(UTF_8));
            long endQuery = System.currentTimeMillis();

            timeToDbQuery += (endQuery - startQuery);

            if(neighboursByteOpt.isPresent()){
                neighbourNodes = getNeighboursWithoutPrefix(neighboursByteOpt.get());
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
        LOGGER.info("TOTAL TIME TO TRAVERSE: {} ms, {} seconds.", endMillis-startMillis, (float)(endMillis - startMillis)/1000 );
        LOGGER.info("Total count of traversed nodes: {}", COUNTER);
        LOGGER.info("Total time to DB queries: {} ms", timeToDbQuery);
        LOGGER.info("Time to BFS without DB queries: {} ms", (endMillis-startMillis) - timeToDbQuery);
        timeToDbQuery = 0;
    }

}
