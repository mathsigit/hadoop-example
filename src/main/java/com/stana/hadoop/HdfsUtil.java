package com.stana.hadoop;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtil {
    private Configuration conf = null;
    private FileSystem fs = null;

    public HdfsUtil(Configuration conf) {
        try {
            this.conf = conf;
            this.fs = FileSystem.get(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void put(String localFilePath, String hdfsFilePath) {
        try {
            Path source = new Path(localFilePath);
            Path dest = new Path(hdfsFilePath);
            if (!fs.exists(dest)) {
                fs.copyFromLocalFile(source, dest);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void append(String hdfsFilePath, String content) {
        File f = new File(hdfsFilePath);
        File parent = f.getParentFile();
        FSDataOutputStream fos;
        try {
            if (parent != null) {
                Files.createDirectories(parent.toPath());
            }

            if (fs.exists(new Path(hdfsFilePath))) {
                fos = fs.append(new Path(hdfsFilePath));
            } else {
                fos = fs.create(new Path(hdfsFilePath), false);
            }
            BufferedWriter writer =
                    new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8));
            writer.append(content);
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            fs.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
