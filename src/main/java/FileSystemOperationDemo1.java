
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class FileSystemOperationDemo1 {

    public static void testfs1() throws IOException {
        Configuration configuration = new Configuration();
        String dfspath = "hdfs://127.0.0.1:9000/TestDir/";
        Path pathdir = new Path(dfspath);
        Path file = new Path(dfspath + "test.txt");
        FileSystem fs = pathdir.getFileSystem(configuration);
        fs.mkdirs(pathdir);
        System.out.println("Write to "+ configuration.get("fs.default.name"));
        fs.copyFromLocalFile(new Path("/Users/dengjq/backup/wordcount/file.txt"),file);
        fs.copyToLocalFile(file,new Path("/Users/dengjq/backup/wordcount/"));
        boolean bn = fs.rename(file, new Path(dfspath + "renamelog.txt"));
        System.out.println(bn);

        DistributedFileSystem hdfs = (DistributedFileSystem)fs;
        DatanodeInfo[] datanodeInfos = hdfs.getDataNodeStats();
        for (DatanodeInfo datanodeInfo : datanodeInfos){
            System.out.println("DataNode节点名称: " + datanodeInfo.getName());
        }
        boolean bndel = fs.delete(pathdir,true);
        System.out.println("删除指定目录下的文件:" + bndel);



        fs.close();
    }

    public static void testfs2() throws IOException {
        Configuration configuration = new Configuration();
        String dfspath = "hdfs://127.0.0.1:9000/user/";
        Path pathdir = new Path(dfspath);
        Path file = new Path(dfspath + "wordcount/file.txt");
        FileSystem fs = pathdir.getFileSystem(configuration);

        System.out.println("开始单个文件状态查看----------");
        FileStatus status = fs.getFileStatus(file);
        System.out.println("获取绝对路径： " + status.getPath());
        System.out.println("获取相对路径： " + status.getPath().toUri().getPath());
        System.out.println("获取block大小： " + status.getBlockSize());
        System.out.println("获取所属组： " + status.getGroup());
        System.out.println("获取所有者： " + status.getOwner());

        System.out.println("开始成批文件状态查看----------");
        FileStatus[] fileStatuses = fs.listStatus(pathdir);
        for (int i = 0; i < fileStatuses.length; i ++){
            System.out.println("文件所在位置：" + fileStatuses[i].getPath().toString());
        }

        fs.close();
    }

    public static void testfs3() throws IOException {
        Configuration configuration = new Configuration();
        String dfspath = "hdfs://127.0.0.1:9000/user/";
        Path pathdir = new Path(dfspath);
        Path file = new Path(dfspath + "wordcount/file.txt");
        FileSystem fs = pathdir.getFileSystem(configuration);

        FSDataInputStream fsin = fs.open(file);
        byte[] buffer = new byte[128];
        int length = 0;
        while((length = fsin.read(buffer,0,128)) != -1){
            System.out.println("缓存数组长度:" + length);
            System.out.println(new String(buffer,0,length));
        }
        System.out.println("length =" + fsin.getPos());
        fsin.seek(5);
        while((length = fsin.read(buffer,0,128)) != -1){
            System.out.println("new 缓存数组长度:" + length);
            System.out.println(new String(buffer,0,length));
        }
        fsin.seek(0);
        byte[] buf2 = new byte[128];
        fsin.read(buf2,0,128);
        System.out.println("buf2=" + new String(buf2));
        System.out.println(buf2.length);
        fs.close();
    }

    public static void testfs4() throws IOException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        String inpath = "hdfs://127.0.0.1:9000/exam1/SampleData.csv";
        String outpath = "hdfs://127.0.0.1:9000/exam1/SampleData.deflate";
        String codecClassname = "org.apache.hadoop.io.compress.DeflateCodec";
        Class<?> codecClass = Class.forName(codecClassname);
        CompressionCodec codec = (DeflateCodec)ReflectionUtils.newInstance(codecClass,configuration);
        FileOutputStream fos = new FileOutputStream(outpath);
        CompressionOutputStream comOut = codec.createOutputStream(fos);
        IOUtils.copyBytes(new FileInputStream(inpath),comOut,1024,false);
        comOut.finish();

    }
    static class ValueComparator implements Comparator<String>{
        Map<String,Double> base;
        public ValueComparator(Map<String,Double> base){
            this.base = base;
        }

        public int compare(String o1, String o2) {
            if(base.get(o1) >= base.get(o2)){
                return -1;
            } else {
                return 1;
            }
        }
    }
    public static void main(String[] args) throws IOException {

        HashMap<String,Double> map = new HashMap<String, Double>();
        ValueComparator bvc = new ValueComparator(map);
        TreeMap<String,Double> sorted_map = new TreeMap<String, Double>(bvc);


        map.put("A",99.5);
        map.put("B",67.4);
        map.put("C",67.4);
        map.put("D",56.1);
        map.put("E",100.1);
        map.put("F",120.5);
        map.put("G",110.2);
        map.put("H",89.6);
        System.out.println("unsorted map:" + map);
        sorted_map.putAll(map);
        System.out.println("results:" + sorted_map);
        map.put("I",80.5);
        sorted_map.put("I",80.5);
        System.out.println("results2:" + sorted_map);

        /*try {
            testfs4();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }*/
    }
}
