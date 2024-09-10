package org.example.sql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.sql.model.Cube;
import org.example.sql.model.Square;

import java.util.ArrayList;
import java.util.List;

public class SparkDataSourceExample {
    private static final SparkSession sparkSession = SparkSession
                                    .builder()
                                    .master("local[*]")
                                    .appName("Datasource example")
                                    .getOrCreate();

    public static void main(String[] args) {
        //        genericLoadSaveFunctions();
        //        genericFileSourceOptions();
        //        parquetFileOperations();
        //        mergingParquetFileSchemas();
        //        jsonFiles();
        //        csvFiles();
    }
    

    private static void genericLoadSaveFunctions() {
        Dataset<Row> usersDF = sparkSession.read().load("src/main/resources/users.parquet");
        usersDF.select("name", "favorite_color").write().save("namesAndFavColors.parquet");

        // Loading a JSON file
        Dataset<Row> peopleDF = sparkSession.read().format("json").load("src/main/resources/people.json");
        peopleDF.select("name", "age").write().save("namesAndAges.parquet");

        // Loading a CSV file
        Dataset<Row> peopleCsvDF = sparkSession.read().format("csv")
                                        .option("sep", ";")
                                        .option("inferSchema", "true")
                                        .option("header", "true")
                                        .load("src/main/resources/people.csv");
        peopleCsvDF.select("name", "age", "job").write().save("namesAgesJobsCsv.parquet");

        // Writing onto a ORC file
        usersDF.write().format("orc")
                                        .option("orc.bloom.filter.columns", "favorite_color")
                                        .option("orc.dictionary.key.threshold", "1.0")
                                        .option("orc.column.encoding.direct", "name")
                                        .save("users_with_options.orc");

        // Writing onto a Parquet file
        usersDF.write().format("parquet")
                                        .option("parquet.bloom.filter.enabled#favorite_color", "true")
                                        .option("parquet.bloom.filter.expected.ndv#favorite_color", "1000000")
                                        .option("parquet.enable.dictionary", "true")
                                        .option("parquet.page.write-checksum.enabled", "false")
                                        .save("users_with_options.parquet");
        // Run SQL directly on files
        sparkSession.sql("SELECT * FROM parquet. `src/main/resources/users.parquet`").show();

        /*
        Saving Persistent Tables
        DataFrames can also be saved as persistent tables into Hive metastore using the saveAsTable command.
        Notice that an existing Hive deployment is not necessary to use this feature. Spark will create a
        default local Hive metastore (using Derby) for you. Unlike the createOrReplaceTempView command,
        saveAsTable will materialize the contents of the DataFrame and create a pointer to the data in the
        Hive metastore. Persistent tables will still exist even after your Spark program has restarted, as
        long as you maintain your connection to the same metastore. A DataFrame for a persistent table can
        be created by calling the table method on a SparkSession with the name of the table.

        For file-based data source, e.g. text, parquet, json, etc. you can specify a custom table path via the
        path option, e.g. df.write.option("path", "/some/path").saveAsTable("t"). When the table is dropped,
        the custom table path will not be removed and the table data is still there. If no custom table path is
        specified, Spark will write data to a default table path under the warehouse directory. When the table is
        dropped, the default table path will be removed too.
         */

        // Bucketing, Sorting and Partitioning
        /*
        For file-based data source, it is also possible to bucket and sort or partition the output.
        Bucketing and sorting are applicable only to persistent tables
        */
        peopleDF.write().bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed");

        // While partitioning can be used with both save() and saveAsTable()
        usersDF.write().partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet");

    }

    private static void genericFileSourceOptions() {
        // Ignore corrupt files
        /*
        Spark allows you to use spark.sql.files.ignoreCorruptFiles to ignore corrupt files while reading
        data from files. When set to true, the Spark jobs will continue to run when encountering corrupted
        files and the contents that have been read will still be returned.
        */

        // Enable ignore corrupt files
        sparkSession.sql("set spark.sql.files.ignoreCorruptFiles=true");

        // dir1/file3.json is corrupt from parquet's view, file is actually not corrupt
        // it's just that it is in JSON format.
        Dataset<Row> corruptDF = sparkSession.read().parquet(
                                        "src/main/resources/dir1",
                                        "src/main/resources/dir1/dir2/"
        );
        corruptDF.show();

        // +-------------+
        // |         file|
        // +-------------+
        // |file1.parquet|
        // |file2.parquet|
        // +-------------+

        // Ignoring missing files
        /*
        Spark allows you to use spark.sql.files.ignoreMissingFiles to ignore missing files while
        reading data from files. Here, missing file really means the deleted file under directory
        after you construct the DataFrame. When set to true, the Spark jobs will continue to run
        when encountering missing files and the contents that have been read will still be returned.
         */

        sparkSession.sql("set spark.sql.files.ignoreMissingFiles");

        // Path Global Filter

        /*
        pathGlobFilter is used to only include files with file names matching the pattern. The syntax
        follows org.apache.hadoop.fs.GlobFilter. It does not change the behavior of partition discovery.

        To load files with paths matching a given glob pattern while keeping the behavior of partition
        discovery, you can use:
         */

        Dataset<Row> testGlobalPathFilterDF = sparkSession.read().format("parquet")
                                        .option("pathGlobFilter", "*.parquet")      // Json files will be excluded.
                                        .load("src/main/resources/dir1");

        testGlobalPathFilterDF.show();

        // +-------------+
        // |         file|
        // +-------------+
        // |file1.parquet|
        // +-------------+

        // Recursive File Lookup
        /*
        recursiveFileLookup is used to recursively load files, and it disables partition inferring.
        Its default value is false. If data source explicitly specifies the partitionSpec when recursiveFileLookup
        is true, exception will be thrown.
        */

        Dataset<Row> recursiveDF = sparkSession.read().format("parquet")
                                        .option("recursiveFileLookup", "true")
                                        .load("src/main/resources/dir1");

        recursiveDF.show();

        // +-------------+
        // |         file|
        // +-------------+
        // |file1.parquet|
        // |file2.parquet|
        // +-------------+

        // Modification Time Path Filters
        /*
        modifiedBefore and modifiedAfter are options that can be applied together or separately in order to
        achieve greater granularity over which files may load during a Spark batch query. (Note that
        Structured Streaming file sources don’t support these options.)

        modifiedBefore: an optional timestamp to only include files with modification times occurring before
        the specified time. The provided timestamp must be in the following format: YYYY-MM-DDTHH:mm:ss
        (e.g. 2020-06-01T13:00:00)
        modifiedAfter: an optional timestamp to only include files with modification times occurring
        after the specified time. The provided timestamp must be in the following format: YYYY-MM-DDTHH:mm:ss
        (e.g. 2020-06-01T13:00:00)
        When a timezone option is not provided, the timestamps will be interpreted according to the Spark
        session timezone (spark.sql.session.timeZone).
         */

        Dataset<Row> beforeFilterDF = sparkSession.read()
                                        .format("parquet")
                                        .option("modifiedBefore", "2024-08-16T15:19:00")
                                        .option("modifiedAfter", "2024-08-13T15:30:00")
                                        .option("timeZone", "CST")
                                        .load("src/main/resources/dir1");

        beforeFilterDF.show();
    }

    private static void parquetFileOperations() {
        Dataset<Row> peopleJsonDF = sparkSession.read().json("src/main/resources/people.json");
        // Dataframes can be saved as parquet files, maintaining the schema information
        peopleJsonDF.write().parquet("people.parquet");

        // Read this parquet file
        Dataset<Row> peopleParquetDF = sparkSession.read().parquet("people.parquet");
        peopleParquetDF.show();

        // Parquet files can also be used to create a temporary view and then
        // used in SQL statements

        peopleParquetDF.createOrReplaceTempView("peopleParquetView");
        Dataset<Row> getPeopleDF = sparkSession.sql("SELECT * FROM peopleParquetView WHERE AGE BETWEEN 13 AND 19");
        Dataset<String> getPeopleName = getPeopleDF.map(
                                        (MapFunction<Row, String>) row -> row.getString(1),
                                        Encoders.STRING()
        );

        getPeopleName.show();
    }

    private static void mergingParquetFileSchemas() {
        /*
        Like Protocol Buffer, Avro, and Thrift, Parquet also supports schema evolution. Users
        can start with a simple schema, and gradually add more columns to the schema as needed.
        In this way, users may end up with multiple Parquet files with different but mutually
        compatible schemas. The Parquet data source is now able to automatically detect this
        case and merge schemas of all these files.

        Since schema merging is a relatively expensive operation, and is not a necessity in most
        cases, we turned it off by default starting from 1.5.0. You may enable it by

        setting data source option mergeSchema to true when reading Parquet files
        (as shown in the examples below), or
        setting the global SQL option spark.sql.parquet.mergeSchema to true.
         */

        List<Square> squares = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            squares.add(new Square(i, i * i));
        }

        // Creating a dataset of Square and write on parquet file.
        Dataset<Row> squareDF = sparkSession.createDataFrame(squares, Square.class);
        squareDF.write().parquet("data/test_table/key=1");

        List<Cube> cubes = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            cubes.add(new Cube(i, i * i * i));
        }

        // Creating a Dataframe of Cubes and writing on parquet file
        Dataset<Row> cubeDF = sparkSession.createDataFrame(cubes, Cube.class);
        cubeDF.write().parquet("data/test_table/key=2");

        // Read the partitioned table
        Dataset<Row> mergedDF = sparkSession.read().option("mergeSchema", true).parquet("data/test_table");
        mergedDF.printSchema();
        mergedDF.show();
        /*
        The final schema consists of all 3 columns in the Parquet files together
        with the partitioning column appeared in the partition directory paths
        root
          |-- value: int (nullable = true)
          |-- square: int (nullable = true)
          |-- cube: int (nullable = true)
          |-- key: int (nullable = true)
         */

    }

    private static void jsonFiles() {
        // A JSON dataset is pointed to by path.
        // The path can be either a single text file or a directory storing text files
        Dataset<Row> people = sparkSession.read().json("src/main/resources/people.json");
        people.show();

        // The inferred schema can be visualized using the printSchema() method
        people.printSchema();
        // root
        //  |-- age: long (nullable = true)
        //  |-- name: string (nullable = true)

        // Alternatively, a DataFrame can be created for a JSON dataset represented by
        // a Dataset<String> storing one JSON object per string.

        List<String> jsonData = List.of(
                                        "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
        Dataset<String> jsonDS = sparkSession.createDataset(jsonData, Encoders.STRING());
        Dataset<Row> jsonDF = sparkSession.read().json(jsonDS);
        jsonDF.show();
    }

    private static void csvFiles() {
        // A CSV dataset is pointed to by path.
        // The path can be either a single CSV file or a directory of CSV files
        Dataset<Row> people = sparkSession.read().csv("src/main/resources/people.csv");
        people.show();

        // Read a CSV with a delimiter, the default character is ,
        Dataset<Row> peopleWithDelimiter = sparkSession.read().option("delimiter", ";")
                                        .csv("src/main/resources/people.csv");
        peopleWithDelimiter.show();

        // Read a csv with delimiter and a header
        Dataset<Row> peopleWithDelimiterAndHeader =
                                        sparkSession.read().option("delimiter", ";").option("header", "true")
                                                                        .csv("src/main/resources/people.csv");
        peopleWithDelimiterAndHeader.show();

    }

}
