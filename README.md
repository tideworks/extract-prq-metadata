# extract-prq-metadata - extracts out common metadata files for directory tree of Parquet files

When invoked pass a directory path to a top-level node to start scanning in descending manner. The assumption is that Parquet files will be typed (based on their source table of origin) into sub-directories where all files there-under are of the same type.

As the first file of a given type is encountered in the scanning processing, its metadata is extracted and written to into a file named `_common_metadata`. This file will be located in the directory that is an umbrella to all the files of same data type represented by the metadata.

Once this first file is encountered, further scanning of its siblings are halted so that scanning will go down another sub-directory until the entire directory tree is exhausted.