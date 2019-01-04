import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.security.PrivilegedExceptionAction;

public class Main {

    private static final String USER_NAME = "testUser";
    private static final String NAMENODE_IP = "35.197.225.120";
    private static final String NAMENODE_PORT = "9000";
    private static final String HADOOP_WORKDIR = "/user/testUser/";

    public static void main(final String[] args){
        try {
            final String[] arguments = args;

            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(USER_NAME);
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception {
                    Configuration conf = new Configuration();
                    conf.set("fs.defaultFS", "hdfs://" + NAMENODE_IP + ":" + NAMENODE_PORT + "/user/"+ USER_NAME);
                    conf.set("hadoop.job.ugi", USER_NAME);
                    conf.set("dfs.client.use.datanode.hostname", "true");

                    FileSystem fs = FileSystem.get(conf);

                    if (arguments[0].equals("write")) {
                        File sourceDir = new File(arguments[1]);
                        System.out.println("Writing files to HDFS from " + sourceDir);

                        for(File file : sourceDir.listFiles()) {
                            fs.copyFromLocalFile(new Path(file.getAbsolutePath()), new Path(HADOOP_WORKDIR));
                        }

                        System.out.println("Writing completed!");
                    } else if(arguments[0].equals("read")) {
                        File destinationDir = new File(arguments[1]);
                        System.out.println("Reading files from HDFS and storing them to: " + destinationDir);

                        FileStatus[] files = fs.listStatus(new Path(HADOOP_WORKDIR));

                        for(FileStatus file : files) {
                            fs.copyToLocalFile(file.getPath(), new Path(destinationDir.getAbsolutePath()));
                        }

                        System.out.println("Read completed!");
                    }

                    System.out.println("Done all! Exiting...");
                    return null;
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}