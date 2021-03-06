package com.dinglicom.chapter03;

import org.elasticsearch.common.recycler.Recycler;

/**
 * @author ly
 * @Date Create in 11:05 2021/2/22 0022
 * @Description
 */
public interface MyHashMap<K,V> {
   public V put(K k, V v);
   public V get(K k);

   interface Entry<K,V>{
       public K getKey();
       public V getValue();
    }
}
