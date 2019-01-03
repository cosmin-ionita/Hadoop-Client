import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Main {

    private static final String NAME_NODE = "hdfs://35.204.35.133:9000";

    public static void main(String[] args) throws URISyntaxException, IOException {
        String fileInHdfs = "/Users/git add ./Desktop/goals.txt";
        FileSystem fs = FileSystem.get(new URI(NAME_NODE), new Configuration());
        String fileContent = IOUtils.toString(fs.open(new Path(fileInHdfs)), "UTF-8");
        System.out.println("File content - " + fileContent);

        System.out.println("Hello World!");
    }
}


