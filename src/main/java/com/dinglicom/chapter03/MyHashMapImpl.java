package com.dinglicom.chapter03;

/**
 * @author ly
 * @Date Create in 13:09 2021/3/5 0005
 * @Description
 */
public class MyHashMapImpl<K,V> implements MyHashMap<K,V> {
    private static final int defalut_size = 1<<4;
    private static final float default_load = 0.75f;

    private int defalutSize ;
    private float defaultLoad;

    int Entrysize;

    Entry<K,V>[] table = null;

    public MyHashMapImpl(){
            this(defalut_size,default_load);
    }

    public MyHashMapImpl(int defalutSize,float defaultLoad){
        this.defalutSize=defalutSize;
        this.defaultLoad=defaultLoad;
        this.table=new Entry[this.defalutSize];
    }

    @Override
    public Object put(Object o, Object o2) {
        return null;
    }

    @Override
    public Object get(Object o) {
        return null;
    }

    class Entry<K,V> implements MyHashMap.Entry{

        private K key;
        private V value;
        private Entry<K,V> next;

        public Entry(){

        }



        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }
    }
}
