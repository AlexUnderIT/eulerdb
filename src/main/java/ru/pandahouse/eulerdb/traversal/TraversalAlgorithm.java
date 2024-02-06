package ru.pandahouse.eulerdb.traversal;

import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public abstract class TraversalAlgorithm {
    public static int COUNTER;
    public List<String> getNeighboursWithoutPrefix(byte[] neighboursByte){
        String[] parseToString = new String(neighboursByte).split(", ");
        return Arrays
                .stream(parseToString)
                .map(i -> i.substring(1)).collect(Collectors.toList());
    }
}
