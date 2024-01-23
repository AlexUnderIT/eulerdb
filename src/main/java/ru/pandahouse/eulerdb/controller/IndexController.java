package ru.pandahouse.eulerdb.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import ru.pandahouse.eulerdb.service.RocksDbService;

import java.sql.SQLOutput;
import java.util.Optional;

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
    @GetMapping("/put/{key}/{value}")
    public void putSomething(@PathVariable("key") String key,
                             @PathVariable("value") Object value){
        rocksDbService.save(key, value);
    }
    @GetMapping("/getCol")
    public void getColumn(){
        rocksDbService.saveColumnFamily();
    }
    @GetMapping("/find/{key}")
    public void getSmthByKey(@PathVariable("key") String key){
        rocksDbService.find(key);
    }
    @GetMapping("/delete/{key}")
    public void deleteSmthByKey(@PathVariable("key") String key){
        rocksDbService.delete(key);
    }
}
