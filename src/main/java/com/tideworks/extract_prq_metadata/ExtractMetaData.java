package com.tideworks.extract_prq_metadata;

import com.tideworks.annotation.InvokeByteCodePatching;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Predicate;

@InvokeByteCodePatching
public class ExtractMetaData {
  private static final Logger LOGGER;
  private static final File progDirPathFile;

  static File getProgDirPath() { return progDirPathFile; }

  static {
    final Predicate<String> existsAndIsDir = dirPath -> {
      final File dirPathFile = new File(dirPath);
      return dirPathFile.exists() && dirPathFile.isDirectory();
    };
    String homeDirPath = System.getenv("HOME"); // user home directory
    homeDirPath = homeDirPath != null && !homeDirPath.isEmpty() && existsAndIsDir.test(homeDirPath) ? homeDirPath : ".";
    progDirPathFile = FileSystems.getDefault().getPath(homeDirPath).toFile();
    LoggingLevel.setLoggingVerbosity(LoggingLevel.DEBUG);
    LOGGER = LoggingLevel.effectLoggingLevel(() -> LoggerFactory.getLogger(ExtractMetaData.class.getSimpleName()));
  }

  public static void main(String[] args) {
    try {
      if (args.length <= 0) {
        LOGGER.error("please supply a directory path to root node of directory tree to be scanned");
        System.exit(1); // return non-zero status to indicate program failure
      }
      final Path dirTreeRootNode = Paths.get(args[0]);
      if (Files.notExists(dirTreeRootNode)) {
        LOGGER.error("directory path \"{}\" does not exist", dirTreeRootNode);
        System.exit(1); // return non-zero status to indicate program failure
      } else if (!Files.isDirectory(dirTreeRootNode)) {
        LOGGER.error("file path \"{}\" is not a directory", dirTreeRootNode);
        System.exit(1); // return non-zero status to indicate program failure
      }
      System.out.printf("Hello World!%nDirectory tree root node: \"%s\"", dirTreeRootNode);
    } catch (Throwable e) {
      LOGGER.error("program terminated due to exception:", e);
      System.exit(1); // return non-zero status to indicate program failure
    }
  }
}
