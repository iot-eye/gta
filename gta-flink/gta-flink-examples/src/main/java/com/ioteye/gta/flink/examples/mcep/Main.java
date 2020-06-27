package com.ioteye.gta.flink.examples.mcep;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        Map<Key, String> map = new HashMap<>();

        Key key1 = new Key();key1.setKey(1); key1.setName("key1"); key1.setAge(1);
        Key key2 = new Key();key2.setKey(1); key1.setName("key2"); key1.setAge(2);
        Key key3 = new Key();key2.setKey(3); key1.setName("key3"); key1.setAge(3);

        map.put(key1, "a");

        map.computeIfAbsent(key2, key -> "b");

        Iterator<Map.Entry<Key, String>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Key, String> next = iterator.next();
            iterator.remove();
        }

        System.out.println(map);
    }

    @Getter
    @Setter
    @ToString
    static class Key {
        Integer key;
        Integer age;
        String name;

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Key) {
                Key key = (Key) obj;
                if (this.key.equals(key.key)) {
                    return true;
                }
            }
            return false;
        }
    }
}
