package ru.pandahouse.eulerdb.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
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
    @GetMapping("/put/{key}/{value}")
    public void putSomething(@PathVariable("key") String key,
                             @PathVariable("value") Object value){
        rocksDbService.save(key, value);
    }
    @GetMapping("/putCol/{name}/{key}/{value}")
    public void putColumnFamilyData(@PathVariable("name") String name,
                          @PathVariable("key") String key,
                          @PathVariable("value") Object value){
        rocksDbService.saveColumnFamily(name, key, value);
    }
    @GetMapping("/getCol/{name}/{key}")
    public void getColumnFamilyData(@PathVariable("name") String name,
                                    @PathVariable("key") String key){
        rocksDbService.findColumnFamilyValue(name, key);
    }
    @GetMapping("/delCol/{name}/{key}")
    public void delColumnFamilyData(@PathVariable("name") String name,
                                    @PathVariable("key") String key){
        rocksDbService.deleteColumnFamilyValue(name, key);
    }
    @GetMapping("/find/{key}")
    public void getByKey(@PathVariable("key") String key){
        rocksDbService.find(key);
    }
    @GetMapping("/delete/{key}")
    public void deleteByKey(@PathVariable("key") String key){
        rocksDbService.delete(key);
    }
    /*@GetMapping("/merge/{key}/{value}")
    public void mergeTest(@PathVariable("key") String key,
                          @PathVariable("value") Object value){
        rocksDbService.mergeTest(key, value);
    }*/
}
