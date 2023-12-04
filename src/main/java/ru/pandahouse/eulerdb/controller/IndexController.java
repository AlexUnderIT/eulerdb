package ru.pandahouse.eulerdb.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.pandahouse.eulerdb.service.RocksDbService;

@RestController
public class IndexController {

    private final RocksDbService rocksDbService;

    public IndexController(
        RocksDbService rocksDbService
    ) {
        this.rocksDbService = rocksDbService;
    }

    @GetMapping("get")
    public void get() {
        rocksDbService.get();
    }

}
