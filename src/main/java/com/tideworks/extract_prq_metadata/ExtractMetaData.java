package com.tideworks.extract_prq_metadata;

import com.tideworks.annotation.InvokeByteCodePatching;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.tideworks.extract_prq_metadata.io.InputFile.nioPathToInputFile;
import static com.tideworks.extract_prq_metadata.io.OutputFile.nioPathToOutputFile;
import static java.lang.Integer.MAX_VALUE;
import static java.nio.file.FileVisitOption.FOLLOW_LINKS;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

@InvokeByteCodePatching
public class ExtractMetaData {
  private static final Logger LOGGER;
  private static final File progDirPathFile;
  private final Set<String> metaDataFilesSet;
  private final Method mergeFooters;

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

  private ExtractMetaData() {
    this.metaDataFilesSet = new LinkedHashSet<>((int) (500d / .75d));
    this.mergeFooters = getMergeFootersStaticMethod();
  }

  private static Method getMergeFootersStaticMethod() {
    Method method;
    try {
      final String methodName = "mergeFooters";
      //noinspection JavaReflectionMemberAccess
      method = ParquetFileWriter.class.getDeclaredMethod(methodName, org.apache.hadoop.fs.Path.class, List.class);
      if ((method.getModifiers() & Modifier.STATIC) == 0) {
        throw new NoSuchMethodException(String.format("static method %s(..) not found", methodName));
      }
      method.setAccessible(true);
    } catch (NoSuchMethodException e) {
      uncheckedExceptionThrow(e);
      method = null; // will never reach here - quites compiler
    }
    return method;
  }

  @SuppressWarnings({"unchecked", "UnusedReturnValue"})
  private static <T extends Throwable, R> R uncheckedExceptionThrow(Throwable t) throws T { throw (T) t; }

  private ParquetMetadata mergeFooters(org.apache.hadoop.fs.Path root, List<Footer> footers) {
    try {
      return (ParquetMetadata) mergeFooters.invoke(null, root, footers);
    } catch (InvocationTargetException | IllegalAccessException e) {
      uncheckedExceptionThrow(e);
      return null; // will never reach here - quites compiler
    }
  }

  public static void main(String[] args) {
    final Supplier<String> noDirPathArgError = () -> {
      LOGGER.error("please supply a directory path to root node of directory tree to be scanned");
      System.exit(1); // return non-zero status to indicate program failure
      return null;
    };
    try {
      if (args.length <= 0) {
        noDirPathArgError.get();
      }
      Optional<String> dirPathOpt = Optional.empty();
      boolean overWriteExisting = false;
      for(String arg : args) {
        if (arg.equalsIgnoreCase("-o")) {
          overWriteExisting = true;
        } else {
          dirPathOpt = Optional.of(arg);
        }
      }
      final Path dirTreeRootNode = Paths.get(dirPathOpt.orElseGet(noDirPathArgError));
      if (Files.notExists(dirTreeRootNode)) {
        LOGGER.error("directory path \"{}\" does not exist", dirTreeRootNode);
        System.exit(1); // return non-zero status to indicate program failure
      } else if (!Files.isDirectory(dirTreeRootNode)) {
        LOGGER.error("file path \"{}\" is not a directory", dirTreeRootNode);
        System.exit(1); // return non-zero status to indicate program failure
      }
      System.out.printf("Directory tree root node: \"%s\"%n%n", dirTreeRootNode);
      new ExtractMetaData().scanDirTreeAndExtractMetaData(dirTreeRootNode, overWriteExisting);
    } catch (Throwable e) {
      LOGGER.error("program terminated due to exception:", e);
      System.exit(1); // return non-zero status to indicate program failure
    }
  }

  private void scanDirTreeAndExtractMetaData(final Path srcDir, final boolean overWriteExisting) throws IOException {
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
          if (!metaDataFilesSet.contains(metaDataOutPath.toString())) {
            System.out.println(metaDataOutPath);
            extractMetaDataFooter(srcDir, visitFilePath, metaDataOutPath, overWriteExisting);
          }
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

  private void extractMetaDataFooter(final Path srcDir,
                                     final Path parquetFilePath,
                                     final Path metaDataOutPath,
                                     final boolean overWriteExisting) throws IOException
  {
    try (final ParquetFileReader rdr = ParquetFileReader.open(nioPathToInputFile(parquetFilePath))) {
      ParquetMetadata metaData = rdr.getFooter();
      if (!overWriteExisting && Files.exists(metaDataOutPath)) {
        final Footer parquetFileFooter = new Footer(new org.apache.hadoop.fs.Path(parquetFilePath.toUri()), metaData);
        final ParquetMetadata priorMetaData = readMetaDataFile(metaDataOutPath);
        final Footer existingFooter = new Footer(new org.apache.hadoop.fs.Path(metaDataOutPath.toUri()), priorMetaData);
        final List<Footer> footers = new ArrayList<>();
        footers.add(existingFooter);
        footers.add(parquetFileFooter);
        metaData = mergeFooters(new org.apache.hadoop.fs.Path(srcDir.toUri()), footers);
      }
      Files.deleteIfExists(metaDataOutPath);
      try (final PositionOutputStream out = nioPathToOutputFile(metaDataOutPath).createOrOverwrite(0)) {
        serializeFooter(metaData, out);
      }
      final boolean wasAdded = metaDataFilesSet.add(metaDataOutPath.toString());
      assert wasAdded;
    }
  }

  private static ParquetMetadata readMetaDataFile(final Path metaDataOutPath) throws IOException {
    try (final InputStream inputStream = Files.newInputStream(metaDataOutPath)) {
      //noinspection ResultOfMethodCallIgnored
      inputStream.skip(ParquetFileWriter.MAGIC.length);
      return new ParquetMetadataConverter().readParquetMetadata(inputStream, NO_FILTER);
    }
  }

  private static void serializeFooter(ParquetMetadata footer, final PositionOutputStream out) throws IOException {
    out.write(ParquetFileWriter.MAGIC);
    final long footerIndex = out.getPos();
    //noinspection unchecked
    footer = new ParquetMetadata(footer.getFileMetaData(), Collections.EMPTY_LIST);
    final ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();
    final org.apache.parquet.format.FileMetaData parquetMetadata =
            metadataConverter.toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer);
    Util.writeFileMetaData(parquetMetadata, out);
    LOGGER.debug("{}: footer length = {}" , out.getPos(), (out.getPos() - footerIndex));
    BytesUtils.writeIntLittleEndian(out, (int) (out.getPos() - footerIndex));
    out.write(ParquetFileWriter.MAGIC);
  }
}
