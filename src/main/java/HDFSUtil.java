import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class HDFSUtil {

    private FileSystem fs;

    private Configuration conf;

    public HDFSUtil() throws IOException {
        conf = new Configuration();
        fs = FileSystem.get(conf);
    }

    /**
     * 创建HDFS目录
     *
     * @param path 创建的目录路径
     * @return true:创建成功
     * @throws IOException
     */
    public boolean mkdir(String path) throws IOException {
        Path srcPath = new Path(path);
        return fs.mkdirs(srcPath);
    }

    /**
     * 递归删除HDFS目录
     *
     * @param path 要删除的HDFS目录
     * @return true:删除成功
     * @throws IOException
     */
    public boolean clearDir(String path) throws IOException {
        Path srcPath = new Path(path);
        return fs.delete(srcPath, true);
    }

    /**
     * 上传本地文件到HDFS
     *
     * @param src 源文件路径
     * @param dst HDFS路径（目标路径）
     * @throws IOException
     */
    public void put(String src, String dst, boolean delSrc, boolean overwrited) throws IOException {
        Path srcPath = new Path(src);
        Path dstPath = new Path(dst); // 目标路径
        // 调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认>为false
        fs.copyFromLocalFile(delSrc, overwrited, srcPath, dstPath);
        // FileUtil.copy(new File(src), fs, dstPath, false, conf);
    }


    /**
     * 修改HDFS目录的读写权限
     *
     * @param src  要修改的HDFS目录
     * @param mode 读写权限(例如：744)
     * @throws IOException
     */
    public void changePermission(Path src, String mode) throws IOException {
        //Path path = new Path(src);
        FsPermission fp = new FsPermission(mode);
        fs.setPermission(src, fp);
    }

    /**
     * 从HDFS下载文件到本地
     *
     * @param src 源文件路径
     * @param dst HDFS路径（目标路径）
     * @throws IOException
     */
    public void get(String src, String dst) throws IOException {
        Path srcPath = new Path(src);
        Path dstPath = new Path(dst); // 目标路径
        fs.copyToLocalFile(srcPath, dstPath);
        // FileUtil.copy(new File(src), fs, dstPath, false, conf);
    }

    /**
     * 校验文件是否存在于HDFS中。
     *
     * @param filePath 文件再HDFS中的路径
     * @return true：存在
     * @throws IOException
     */
    public boolean check(String filePath) throws IOException {
        Path path = new Path(filePath);
        boolean isExists = fs.exists(path);
        return isExists;
    }

    /**
     * 向HDFS文件中追加内容
     *
     * @param filePath 文件路径
     * @return 是否追加成功。
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public boolean appendContent(InputStream in, String filePath) throws IOException {
        conf.setBoolean("dfs.support.append", true);
        if (!check(filePath)) {
            fs.createNewFile(new Path(filePath));

        }
        //要追加的文件流，inpath为文件
        OutputStream out = fs.append(new Path(filePath));
        IOUtils.copyBytes(in, out, 10, true);
        in.close();
        out.close();

        fs.close();
        return false;
    }

    /**
     * 查看指定目录下的所有文件
     *
     * @param filePath HDFS目录路径
     * @return 指定路径下所有文件列表
     * @throws IOException
     */
    public List<String> listFile(String filePath, String ext) throws IOException {
        List<String> listDir = new ArrayList<String>();
        Path path = new Path(filePath);
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, true);
        while (it.hasNext()) {
            String name = it.next().getPath().toString();
            if (name.endsWith(ext)) {
                listDir.add(name);
            }
        }
        return listDir;
    }

    /**
     * 得到一个目录(不包括子目录)下的所有名字匹配上pattern的文件名
     *
     * @param fs
     * @param folderPath
     * @param pattern    用于匹配文件名的正则
     * @return
     * @throws IOException
     */
    public List<Path> getFilesUnderFolder(FileSystem fs, Path folderPath,
                                          String pattern) throws IOException {
        List<Path> paths = new ArrayList<Path>();
        if (fs.exists(folderPath)) {
            FileStatus[] fileStatus = fs.listStatus(folderPath);
            for (int i = 0; i < fileStatus.length; i++) {
                FileStatus fileStatu = fileStatus[i];
                // 如果是目录，递归向下找
                if (fileStatu.isDir()) {
                    Path oneFilePath = fileStatu.getPath();
                    if (pattern == null) {
                        paths.add(oneFilePath);
                    } else {
                        if (oneFilePath.getName().contains(pattern)) {
                            paths.add(oneFilePath);
                        }
                    }
                }
            }
        }
        return paths;
    }

    /**
     * 移动HDFS文件到另外一个目录
     *
     * @param src  源文件
     * @param dest 目标文件
     * @throws IOException
     */
    public void moveFile(String src, String dest) throws IOException {
        Path srcPath = new Path(src);
        Path destPath = new Path(dest);
//		if(this.check(dest)) {
//			log.error("文件" + dest + "已经存在，无法覆盖！");
//			return;
//		}
        fs.rename(srcPath, destPath);
    }

    /**
     * 移动HDFS文件到另外一个目录
     *
     * @param src  源文件
     * @param dest 目标文件
     * @throws IOException
     */
    public void copyFile(String src, String dest) throws IOException {
        Path srcPath = new Path(src);
        Path destPath = new Path(dest);
//		if(this.check(dest)) {
//			log.error("文件" + dest + "已经存在，无法覆盖！");
//			return;
//		}
        fs.rename(srcPath, destPath);
    }

    public OutputStream getHDFSOutputStream(String path) throws IllegalArgumentException, IOException {
        FSDataOutputStream dataOutStream = null;
        dataOutStream = fs.create(new Path(path), true, 1024 * 1024 * 1024);
        return dataOutStream;
    }

    // 释放资源
    public void destroy() throws IOException {
        if (fs != null)
            fs.close();
    }

    public FileSystem getFs() {
        return fs;
    }

    public void setFs(FileSystem fs) {
        this.fs = fs;


    }

    /*
    * 统计目录下的文件数，空间占用情况
    *
    * */
    public ContentSummary getFileStatuses(String path) {
        ContentSummary fileStatuses = null;
        try {
            fileStatuses = fs.getContentSummary(new Path(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileStatuses;
    }

    /*
    * 递归更改一个文件的所有者
    *
    * */
    public void changeFilesOwner(String path, String owner) {
        try {
            fs.setOwner(new Path(path), owner, "root");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        HDFSUtil hdfsUtil = null;
        try {
            /*
            *1. 创建文件夹data/xuzhengchuang
            * */
            hdfsUtil = new HDFSUtil();
//            hdfsUtil.mkdir("/data");
//            hdfsUtil.mkdir("/data/xuzhengchuang");
            /*
            * 2.上传本地文件夹到HDFS
            * */
//            String src = "D:/test";
//            String dest = "/data/xuzhengchuang";
//            hdfsUtil.put(src, dest, false, true);
        /*
        * 3.查看/data/姓名/目录下文件列表
        * */

//            List<String> fileList = hdfsUtil.listFile("/data", "");
//            for (String str : fileList) {
//                System.out.println(str);
//            }
/*
*
* 4.查看目录下的文件数，和空间占用情况
*
* */
//
//            String path = "/data/xuzhengchuang/test";
//            ContentSummary contentSummary = hdfsUtil.getFileStatuses(path);
//            System.out.println("当前目录下的文件数目是：" + contentSummary.getFileCount());
//            System.out.println("文件大小是：" + contentSummary.getLength());

/**
 *
 * 5.递归更改一个文件所有者
 */
//            String path = "/data/xuzhengchuang";
//            String owner = "root";
//            hdfsUtil.changeFilesOwner(path, owner);
/*
*6. 递归修改文件夹的acl权限
* */
//            String src = "/data/xuzhengchuang/word.txt";
//            Path path = new Path(src);
//            String acl = "744";
//            hdfsUtil.changePermission(path, acl);
/**
 *
 * 7.创建一个大小为0的文件
 *
 */
//            hdfsUtil.getFs().createNewFile(new Path(src));


            /*
            *
            * 8.给文件追加内容
            * */
//            BufferedInputStream in = new BufferedInputStream(new ByteArrayInputStream("Welcome To Hadoop".getBytes()));
//            OutputStream out = hdfsUtil.getFs().append(new Path(src));
//            IOUtils.copyBytes(in, out, 10, true);
//            out.close();
//            in.close();



            /*
            * 9.查看文件中的内容
            * */
            String src = "/data/xuzhengchuang/word.txt";
            InputStream inputStream = hdfsUtil.getFs().open(new Path(src));
            byte[] bytes = new byte[1024];
            int count = -1;
            while (-1 != (count = inputStream.read(bytes))) {
                System.out.println(new String(bytes));
            }
            inputStream.close();


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                //释放资源
                hdfsUtil.destroy();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
