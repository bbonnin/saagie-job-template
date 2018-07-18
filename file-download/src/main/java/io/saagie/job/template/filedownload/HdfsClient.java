package io.saagie.job.template.filedownload;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class HdfsClient {

    private FileSystem hdfs;

    public HdfsClient(String hdfsUrl, String user) throws IOException {
        initHdfs(hdfsUrl, user);
    }

    private void initHdfs(String hdfsUrl, String user) throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("ipc.client.connect.timeout", "2000");
        conf.set("ipc.client.connect.max.retries", "1");
        conf.set("ipc.client.connect.retry.interval", "1000");

        System.setProperty("HADOOP_USER_NAME", user);
        System.setProperty("hadoop.home.dir", "/");

        hdfs = FileSystem.get(URI.create(hdfsUrl), conf);
    }

    public String readAsString(String fileName) throws IOException {

        Path hdfsPath = new Path(fileName);
        FSDataInputStream is = hdfs.open(hdfsPath);
        String content = IOUtils.toString(is, "UTF-8");

        is.close();

        return content;
    }

    public void write(String localFileName, String destFileName) throws IOException {

        Path hdfsPath = new Path(destFileName);
        FSDataOutputStream out = hdfs.create(hdfsPath);

        IOUtils.copy(new FileInputStream(localFileName), out);

        out.close();
    }

    public InputStream open(String fileName) throws IOException {
        return hdfs.open(new Path(fileName));
    }
}
