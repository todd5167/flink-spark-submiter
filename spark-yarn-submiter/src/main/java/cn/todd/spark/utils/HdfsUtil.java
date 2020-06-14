/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.todd.spark.utils;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *  HDFS文件操作
 * Date: 2020/6/14
 * @author todd
 */
public class HdfsUtil {
    /**
     * 文件上传
     * @param conf
     * @param src
     * @param dst
     * @throws IOException
     */
    public static String uploadFile(Configuration conf, String src, String dst) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(src);
        Path dstPath = new Path(dst);
        //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
        fs.copyFromLocalFile(false, srcPath, dstPath);

        //打印文件路径
        System.out.println("Upload to " + conf.get("fs.default.name"));
        System.out.println("------------list files------------" + "\n");
        FileStatus[] fileStatus = fs.listStatus(dstPath);
        Arrays.stream(fileStatus).forEach(System.out::println);
        fs.close();

        return fileStatus.length > 0 ? fileStatus[0].getPath().toString() : "";
    }


    /**
     *  创建文件夹
     * @param conf
     * @param path
     * @throws IOException
     */
    public static void mkdir(Configuration conf, String path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(path);
        if (fs.exists(srcPath)) {
            return;
        }

        boolean success = fs.exists(srcPath);
        if (success) {
            System.out.println("create dir " + path + " success!");
        } else {
            System.out.println("create dir " + path + " failure");
        }
        fs.close();
    }

    /**
     *  查看目录下的文件
     * @param conf
     * @param direPath
     */
    public static void getDirectoryFromHdfs(Configuration conf, String direPath) {
        try {
            FileSystem fs = FileSystem.get(URI.create(direPath), conf);
            FileStatus[] filelist = fs.listStatus(new Path(direPath));

            for (int i = 0; i < filelist.length; i++) {
                System.out.println("_________" + direPath + "目录下所有文件______________");
                FileStatus fileStatus = filelist[i];
                System.out.println("Name:" + fileStatus.getPath().getName());
                System.out.println("Size:" + fileStatus.getLen());
                System.out.println("Path:" + fileStatus.getPath());
            }
            fs.close();
        } catch (Exception e) {
            System.out.println(e.getStackTrace());
        }
    }

}
