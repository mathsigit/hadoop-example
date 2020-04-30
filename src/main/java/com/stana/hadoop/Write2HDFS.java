package com.stana.hadoop;

import org.apache.hadoop.conf.Configuration;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class Write2HDFS {
    public static void main(String[] args) throws Exception {


        Properties properties = new Properties();
        Configuration conf = new Configuration();
        try {
            properties.load(Write2HDFS.class.getResourceAsStream("/config.properties"));
            conf.addResource(Write2HDFS.class.getResourceAsStream(properties.getProperty("hdfs.site.conf")));

            File flightFolder = new File(properties.getProperty("data.file.path"));
            File[] allFlightInfo = flightFolder.listFiles();
            writeContentToHdfs(conf, properties, allFlightInfo);
        } catch (IOException e) {
            throw new Exception(e);
        }
    }
    public static void writeContentToHdfs(
            Configuration conf, Properties properties, File[] allFlightInfo) {
        HdfsUtil hdfsUtil = new HdfsUtil(conf);
        try {
            String hdfsFile4RootPath = properties.getProperty("hdfs.root.dir") + System.currentTimeMillis() + "/";
            for (File flightInfo : allFlightInfo) {
                hdfsUtil.put(flightInfo.getAbsolutePath(), hdfsFile4RootPath + flightInfo.getName());
            }

        } catch (OutOfMemoryError e) {
            throw new RuntimeException(e);
        } finally {
            hdfsUtil.close();
        }
    }

}
