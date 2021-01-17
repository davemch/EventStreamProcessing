package sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
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

    // For some reason some of the downloaded .zip-files are empty.
    // We found them by logging the IOException (`zig file empty`) and afterwards grepping them to receive this list.
    private final String[] emptyFiles = {
            "./downloader/files/20200515151500.export.CSV.zip",
            "./downloader/files/20200611051500.export.CSV.zip",
            "./downloader/files/20200611054500.export.CSV.zip",
            "./downloader/files/20200924031500.export.CSV.zip",
            "./downloader/files/20200924040000.export.CSV.zip",
            "./downloader/files/20200924043000.export.CSV.zip",
            "./downloader/files/20200924050000.export.CSV.zip",
            "./downloader/files/20200924060000.export.CSV.zip",
            "./downloader/files/20200924061500.export.CSV.zip",
            "./downloader/files/20200924063000.export.CSV.zip",
            "./downloader/files/20200924064500.export.CSV.zip",
            "./downloader/files/20200924070000.export.CSV.zip",
            "./downloader/files/20200924071500.export.CSV.zip"
    };

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
            System.out.println("NullPointerException in ZipLoader.run() for directory path: " + directoryPathName);
            e.printStackTrace();
            return;
        }

        // Open each file sequentially and provide line by line to sourceCollector
        String line;
        for(Path path : pathList) {
            try {
                // Check if file is empty
                boolean fileEmpty = false;
                for (String emptyFile : this.emptyFiles) {
                    if (path.toString().equals(emptyFile)) {
                        fileEmpty = true;
                        break;
                    }
                }

                if (fileEmpty) {
                    System.out.println("Skip empty file: " + path.toString());
                    continue;
                }
                System.out.println("Read file: " + path.toString());

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
            } catch (IOException e) {
                    System.out.println("IOException in ZipLoader.run() for file: " + path.toString());
                    e.printStackTrace();
                    continue; // NOTE: We continue here as no harm was done
            }
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

