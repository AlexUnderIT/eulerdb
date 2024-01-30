package ru.pandahouse.eulerdb.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import ru.pandahouse.eulerdb.service.RocksDbService;

import java.nio.charset.StandardCharsets;
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
                             @PathVariable("value") String value){
        rocksDbService.save(key.getBytes(), value.getBytes());
    }
    @GetMapping("/find/{key}")
    public void getByKey(@PathVariable("key") String key){
        rocksDbService.find(key.getBytes());
    }
    @GetMapping("/delete/{key}")
    public void deleteByKey(@PathVariable("key") String key){
        rocksDbService.delete(key.getBytes());
    }
    @GetMapping("/add/{key}/{value}")
    public void mergeTest(@PathVariable("key") String key,
                          @PathVariable("value") String value){
        rocksDbService.add(key.getBytes(),value.getBytes());
    }
    @GetMapping("/putCol/{name}/{key}/{value}")
    public void putColumnFamilyData(@PathVariable("name") String name,
                          @PathVariable("key") String key,
                          @PathVariable("value") String value){
        rocksDbService.saveColumnFamily(name, key.getBytes(StandardCharsets.UTF_8), value.getBytes());
    }
    @GetMapping("/findCol/{name}/{key}")
    public void getColumnFamilyData(@PathVariable("name") String name,
                                    @PathVariable("key") String key){
        rocksDbService.findColumnFamilyValue(name, key.getBytes());
    }
    @GetMapping("/delCol/{name}/{key}")
    public void delColumnFamilyData(@PathVariable("name") String name,
                                    @PathVariable("key") String key){
        rocksDbService.deleteColumnFamilyValue(name, key.getBytes());
    }
    @GetMapping("/addCol/{name}/{key}/{value}")
    public void addColumnFamilyData(@PathVariable("name") String name,
                                    @PathVariable("key") String key,
                                    @PathVariable("value") String value){
        rocksDbService.addColumnFamilyValueByKey(name,key.getBytes(),value.getBytes());
    }
    @GetMapping("/col/find")
    public void multipleColFindTest(){
        rocksDbService.findMultipleColumnFamilyValues(List.of("1234".getBytes(),
                "2345".getBytes(),"3456".getBytes()));
    }
    @GetMapping("col/del/{name}")
    public void multipleColDelTest(@PathVariable("name") String name){
        rocksDbService.deleteMultipleColumnFamilyValues(name,"1111".getBytes(),"2222".getBytes());
    }
    @GetMapping("/getMulti")
    public void getMultiByKeyList(){
        rocksDbService.findMultipleValues(List.of("1111".getBytes(),"2222".getBytes(),"3333".getBytes()));
    }
    @GetMapping("/delMulti")
    public void delMultiByKeyList(){
        rocksDbService.deleteMultipleKeys("1111".getBytes(), "3333".getBytes());
    }
}
