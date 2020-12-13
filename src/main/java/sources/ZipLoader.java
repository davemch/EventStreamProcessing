package sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

/**
 * Read a directory containing zipped GDELT files (*.export.CSV.zip), sort it according to file name
 * in natural order and extract line by line of each unzipped file.
 */
public class ZipLoader implements SourceFunction<String> {
    private volatile boolean isRunning = true;

    private final String directoryPathName;

    public ZipLoader(String directoryPathName) {
        this.directoryPathName = directoryPathName;
    }

    public void run(SourceContext<String> sourceContext){
        List<Path> pathList = new ArrayList<>();

        // Save the path of each file in a sorted List
        try {
            pathList = getFileNames(new File(directoryPathName));
            pathList.sort(Comparator.naturalOrder());
        } catch (NullPointerException e) {
            // TODO: What to do?
            System.out.println("NullPointerException in ZipLoader.run()");
            return;
        }

        // Open each file sequentially and provide line by line to sourceCollector
        String line;
        try {
            for(Path path : pathList) {
                // TODO: Weird thing... Zip files are empty...
                if (path.toString().equals("./src/main/resources/black-lives-matter/20200611051500.export.CSV.zip")
                    || path.toString().equals("./src/main/resources/black-lives-matter/20200611054500.export.CSV.zip")) {
                    continue;
                }

                // Unzip file and provide content in BufferedReader
                ZipFile zipFile = new ZipFile(path.toString());
                ZipEntry content = zipFile.getEntry(path.getFileName().toString().replaceAll(".zip", ""));
                BufferedReader br = new BufferedReader(
                        new InputStreamReader(zipFile.getInputStream(content), StandardCharsets.UTF_8));

                // Read reader line by line
                while ((line = br.readLine()) != null) {
                    sourceContext.collect(line);
                }

                // Close resources
                br.close();
                zipFile.close();
            }
        } catch (IOException e) {
            // TODO: What to do?
            System.out.println("IOException in ZipLoader.run()");
            return;
        }
    }

    public void cancel() {
        this.isRunning = false;
    }

    /**
     * getFileName returns a list of all files in given directory.
     */
    private List<Path> getFileNames(File fileDirectoryPath) {
        List<Path> pathList = new ArrayList<>();
        File[] fileNames = fileDirectoryPath.listFiles();

        // NOTE: We don't check for null here as Exception is caught in caller
        for (File csv : fileNames) {
            pathList.add(csv.toPath());
        }

        return pathList;
    }
}

