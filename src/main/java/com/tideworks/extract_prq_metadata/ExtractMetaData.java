package com.tideworks.extract_prq_metadata;

import com.tideworks.annotation.InvokeByteCodePatching;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;

import static com.tideworks.extract_prq_metadata.io.InputFile.nioPathToInputFile;
import static com.tideworks.extract_prq_metadata.io.OutputFile.nioPathToOutputFile;
import static java.lang.Integer.MAX_VALUE;
import static java.nio.file.FileVisitOption.FOLLOW_LINKS;

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
      System.out.printf("Directory tree root node: \"%s\"%n%n", dirTreeRootNode);
      scanDirTreeAndExtractMetaData(dirTreeRootNode);
    } catch (Throwable e) {
      LOGGER.error("program terminated due to exception:", e);
      System.exit(1); // return non-zero status to indicate program failure
    }
  }

  private static void scanDirTreeAndExtractMetaData(final Path srcDir) throws IOException {
    final Set<FileVisitOption> visitOptions = Collections.singleton(FOLLOW_LINKS);
    Files.walkFileTree(srcDir, visitOptions, MAX_VALUE, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        System.out.printf("%s%c%n", dir, File.separatorChar);
        return FileVisitResult.CONTINUE;
      }
      @Override
      public FileVisitResult visitFile(Path visitFilePath, BasicFileAttributes attrs) throws IOException {
        if (attrs.isDirectory() && !Files.isSameFile(visitFilePath, srcDir)) {
          System.out.printf("%s/%n", visitFilePath);
          return FileVisitResult.CONTINUE;
        } else {
          final String fileName = visitFilePath.getFileName().toString();
          if (!fileName.toLowerCase().endsWith(".parquet")) {
            return FileVisitResult.CONTINUE;
          }
          System.out.println(visitFilePath);
          final Path typeBaseDirPath = getOdsTableTypeBaseDir(srcDir, visitFilePath.getParent());
          final Path metaDataOutPath = Paths.get(typeBaseDirPath.toString(), ParquetFileWriter.PARQUET_COMMON_METADATA_FILE);
          System.out.println(metaDataOutPath);
          extractMetaDataFooter(visitFilePath, metaDataOutPath);
          return FileVisitResult.SKIP_SIBLINGS;
        }
      }
      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        return Files.isSameFile(dir, srcDir) ? FileVisitResult.TERMINATE : FileVisitResult.CONTINUE;
      }
    });
  }

  private static Path getOdsTableTypeBaseDir(final Path srcDir, final Path parent) throws IOException {
    Path last = parent;
    for(Path dir = parent; !Files.isSameFile(dir, srcDir); dir = dir.getParent()) {
      last = dir;
    }
    return last;
  }

  private static void extractMetaDataFooter(final Path parquetFilePath, final Path metaDataOutPath) throws IOException {
    try (final ParquetFileReader rdr = ParquetFileReader.open(nioPathToInputFile(parquetFilePath))) {
      final ParquetMetadata footer = rdr.getFooter();
      Files.deleteIfExists(metaDataOutPath);
      try (final PositionOutputStream out = nioPathToOutputFile(metaDataOutPath).createOrOverwrite(0)) {
        serializeFooter(footer, out);
      }
    }
  }

  private static void serializeFooter(final ParquetMetadata footer, final PositionOutputStream out) throws IOException {
    out.write(ParquetFileWriter.MAGIC);
    final long footerIndex = out.getPos();
    final ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();
    final org.apache.parquet.format.FileMetaData parquetMetadata =
            metadataConverter.toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer);
    Util.writeFileMetaData(parquetMetadata, out);
    LOGGER.debug("{}: footer length = {}" , out.getPos(), (out.getPos() - footerIndex));
    BytesUtils.writeIntLittleEndian(out, (int) (out.getPos() - footerIndex));
    out.write(ParquetFileWriter.MAGIC);
  }
}