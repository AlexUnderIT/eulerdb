package ru.pandahouse.eulerdb.repository;

import java.util.List;
import java.util.Optional;

public interface KVRepository<K, V> {
    boolean save(K key, V value);

    Optional<V> find(K key);

    boolean delete(K key);

    //boolean add(K key, V value);
}
