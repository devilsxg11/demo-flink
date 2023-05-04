package com.demo.flink.catalog;


import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class HiveCatalogDemo {

    public static void main(String[] args) {
        Catalog catalog = new HiveCatalog("myhive", null, "<path_of_hive_conf>");

    }
}
