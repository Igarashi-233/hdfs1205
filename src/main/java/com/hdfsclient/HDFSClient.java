package com.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSClient {

    private FileSystem fs;

    @Before
    public void before() throws IOException, InterruptedException {
        fs = FileSystem.get(URI.create("hdfs://hadoop102:9000") ,new Configuration() ,"IGARASHI" );
        System.out.println("Before!!!!!!!!!!!!");
    }

    @Test
    public void put() throws IOException {
        fs.copyFromLocalFile(new Path("D:\\IGARASHI\\Test1.txt"), new Path("/"));
    }

    @Test
    public void get() throws IOException, InterruptedException {

        //获取一个HDFS的抽象封装对象
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"), configuration, "IGARASHI");

        //用这个对象操作文件系统
        fileSystem.copyToLocalFile(new Path("/test"), new Path("D:\\IGARASHI"));

        //关闭文件系统
        fileSystem.close();
    }

    @Test
    public void rename() throws IOException, InterruptedException {
        //获取文件系统
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"), new Configuration(), "IGARASHI");

        //操作
        fileSystem.rename(new Path("/test"), new Path("/test2"));

        //关闭文件系统
        fileSystem.close();
    }

    @Test
    public void delete() throws IOException {
        boolean delete = fs.delete(new Path("/Test1.txt"), true);
        if(delete){
            System.out.println("删除成功");
        }else {
            System.out.println("删除失败");
        }
    }

    @Test
    public void du() throws IOException {
        FSDataOutputStream append = fs.append(new Path("/test2/1.txt"), 1024);
        FileInputStream open = new FileInputStream("D:\\IGARASHI\\Test1.txt");
        IOUtils.copyBytes(open, append, 1024, true);
    }

    @Test
    public void ls() throws IOException {
        //文件和文件夹都可查询
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

        for(FileStatus fileStatus : fileStatuses){
            if(fileStatus.isFile()){
                System.out.println("以下信息是一个文件的信息");
                System.out.println(fileStatus.getPath());
                System.out.println(fileStatus.getLen());
                System.out.println(fileStatus.getPermission());
            }else{
                System.out.println("一下信息是一个文件夹的信息");
                System.out.println(fileStatus.getPath());
                System.out.println(fileStatus.getLen());
                System.out.println(fileStatus.getPermission());
            }
        }
    }

    @Test
    public void listFiles() throws IOException {
        //只查询文件,（文件夹不存在Block）
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);

        while(files.hasNext()){
            LocatedFileStatus file = files.next();

            System.out.println("=======================");
            System.out.println(file.getPath());

            System.out.println("块信息: ");
            BlockLocation[] blockLocations = file.getBlockLocations();
            for(BlockLocation blockLocation : blockLocations){
                String[] hosts = blockLocation.getHosts();
                System.out.print("块在: ");
                for(String host : hosts){
                    System.out.print(host + " ");
                }
                System.out.println();
            }
        }
    }

    @After
    public void after() throws IOException {
        System.out.println("After!!!!!!!!!!");
        fs.close();
    }
}

