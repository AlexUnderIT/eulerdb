package ru.pandahouse.eulerdb.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import ru.pandahouse.eulerdb.service.RocksDbService;

import java.util.List;

@RestController
public class IndexController {

    private final RocksDbService rocksDbService;

    public IndexController(
        RocksDbService rocksDbService
    ) {
        this.rocksDbService = rocksDbService;
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
    @GetMapping("/merge/{key}/{value}")
    public void mergeTest(@PathVariable("key") String key,
                          @PathVariable("value") Object value){
        rocksDbService.mergeTest(key, value);
    }
    @GetMapping("/add/{key}/{value}")
    public void addValue(@PathVariable("key") String key,
                         @PathVariable("value") Object value){
        rocksDbService.add(key, value);
    }
    @GetMapping("/col/find")
    public void multipleColFindTest(){
        rocksDbService.findMultipleColumnFamilyValues(List.of("1234","2345","3456"));
    }
    @GetMapping("col/del/{name}")
    public void multipleColDelTest(@PathVariable("name") String name){
        rocksDbService.deleteMultipleColumnFamilyValues(name,"1111","2222");
    }
    @GetMapping("/getMulti")
    public void getMultiByKeyList(){
        rocksDbService.findMultipleValues(List.of("1111","2222","3333"));
    }
    @GetMapping("/delMulti")
    public void delMultiByKeyList(){
        rocksDbService.deleteMultipleKeys("1111", "3333");
    }
}
