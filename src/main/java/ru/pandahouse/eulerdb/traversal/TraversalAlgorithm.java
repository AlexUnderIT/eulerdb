package ru.pandahouse.eulerdb.traversal;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

public abstract class TraversalAlgorithm {
    protected static int COUNTER = 0;

    protected static long timeToDbQuery = 0;

    public List<String> getNeighboursWithoutPrefix(byte[] neighboursByte){
        String[] parseToString = new String(neighboursByte).split(", ");
        return Arrays
                .stream(parseToString)
                .map(i -> i.substring(1)).collect(Collectors.toList());
    }
}
