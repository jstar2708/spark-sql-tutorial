package org.example.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.example.sql.model.Person;

import javax.xml.crypto.Data;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;


public class SparkSessionExample {
    private static final SparkSession sparkSession = new SparkSession
                                    .Builder()
                                    .master("local[*]")
                                    .appName("My First Spark SQL Program")
                                    .getOrCreate();
    public static void main(String[] args) {
//        basicSparkSQL();
//        runningDatasetExamples();
//        interoperatingWithRDDsReflection();
        interoperatingWithRDDsProgrammatically();
    }
    private static void basicSparkSQL() {
        /*
         Each line must contain a separate, self-contained valid JSON object,
         i.e. -> { name : "David", age : 24 }

         +----+-------+
         | age|   name|
         +----+-------+
         |null|Michael|
         |  30|   Andy|
         |  19| Justin|
         +----+-------+
         */
        Dataset<Row> dataset = sparkSession.read().json("src/main/resources/people.json");
        // Displays the content of the DataFrame to stdout
        dataset.show();

        // To read a JSON File having objects in multiple lines then add the support for multiline
        Dataset<Row>dataset2 = sparkSession.read().option("multiline","true").json("src/main/resources/random_data.json");
        System.out.println(dataset2.count());
        dataset2.show(5);


        // Printing the schema of the DataFrame

        // root
        // |-- age: long (nullable = true)
        // |-- name: string (nullable = true)
        dataset2.printSchema();

        // Selecting only one column and displaying it.

        // +-------+
        // |   name|
        // +-------+
        // |Michael|
        // |   Andy|
        // | Justin|
        // +-------+
        dataset2.select("name").show(4);

        // Select everybody, but increment the age by 1
        dataset2.select(col("name"), col("age").plus(1)).show(2);

        // Filter people greater than 21
        dataset2.filter(col("age").gt(21)).show(5);

        // Counting people by age
        dataset2.groupBy("age").count().show(5);

        // Register the DataFrame as a SQL temporary view
        dataset2.createOrReplaceTempView("people");
        Dataset<Row> viewDataset = sparkSession.sql("SELECT * FROM people");
        System.out.println("Displaying the Temporary view 'people' : ");
        viewDataset.show();

        /*
        Temporary views in Spark SQL are session-scoped and will disappear if the
        session that creates it terminates. If you want to have a temporary view that
        is shared among all sessions and keep alive until the Spark application
        terminates, you can create a global temporary view. Global temporary view
        is tied to a system preserved database global_temp, and we must use the
        qualified name to refer it, e.g. SELECT * FROM global_temp.view1.
         */

        // Creating a Global Temporary View
        dataset.createOrReplaceGlobalTempView("peopleG");
        Dataset<Row> globalTempView = sparkSession.sql("SELECT * FROM global_temp.peopleG");
        globalTempView.show();
        // View accessible from another session
        sparkSession.newSession().sql("SELECT * FROM global_temp.peopleG");
    }

    private static void runningDatasetExamples() {
        /*
        Datasets are similar to RDDs, however, instead of using Java serialization or
        Kryo they use a specialized Encoder to serialize the objects for processing
        or transmitting over the network. While both encoders and standard serialization
        are responsible for turning an object into bytes, encoders are code generated
        dynamically and use a format that allows Spark to perform many operations like
        filtering, sorting and hashing without deserializing the bytes back into an
        object.
        */

        // Creating instance of bean class
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);
        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> personDataset = sparkSession.createDataset(
                                        Collections.singletonList(person),
                                        personEncoder
        );

        // +---+----+
        // |age|name|
        // +---+----+
        // | 32|Andy|
        // +---+----+
        personDataset.show();

        // Encoders for most common types are provided in class Encoders
        Encoder<Long> longEncoder = Encoders.LONG();
        Dataset<Long> longDataset = sparkSession.createDataset(
                                        List.of(1L, 2L, 3L),
                                        longEncoder
        );
        Dataset<Long> transformedDataset = longDataset.map((MapFunction<Long, Long>) v -> v + 1L, longEncoder);
        transformedDataset.show();

        // DataFrames can be converted to a Dataset by providing a class. Mapping based on name
        String path = "src/main/resources/people.json";
        Dataset<Person> personDataset1 = sparkSession.read().json(path).as(personEncoder);
        personDataset1.show();
    }

    private static void interoperatingWithRDDsReflection() {
        /*
        Spark SQL supports two different methods for converting existing RDDs into Datasets. The first method uses
        reflection to infer the schema of an RDD that contains specific types of objects. This reflection-based
        approach leads to more concise code and works well when you already know the schema while writing your
        Spark application.

        The second method for creating Datasets is through a programmatic interface that allows you to construct
        a schema and then apply it to an existing RDD. While this method is more verbose, it allows you to
        construct Datasets when the columns and their types are not known until runtime.
        */

        // Inferring the Schema using Reflection
        /*
        Spark SQL supports automatically converting an RDD of JavaBeans into a DataFrame. The BeanInfo, obtained using
        reflection, defines the schema of the table. Currently, Spark SQL does not support JavaBeans that contain Map
        field(s). Nested JavaBeans and List or Array fields are supported though. You can create a JavaBean by
        creating a class that implements Serializable and has getters and setters for all of its fields.
         */

        // Create an RDD of Person objects from a text file
        String filePath = Path.of("src", "main", "resources", "people.txt").toString();
        JavaRDD<Person> personRDD = sparkSession.read()
                                        .textFile(filePath)
                                        .javaRDD()
                                        .map(line -> {
                                            final var lineArr = line.split(",");
                                            return new Person(lineArr[0], Long.parseLong(lineArr[1].trim()));
                                        })
                                        ;

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> dataFrame = sparkSession.createDataFrame(personRDD, Person.class);
        // Register the DataFrame as a temporary view
        dataFrame.createOrReplaceTempView("personView");
        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> teenagers = sparkSession.sql("SELECT * FROM personView WHERE age BETWEEN 13 AND 19");

        // The columns of a row in the result can be accessed by field index
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagersNamesDF = teenagers.map(
                                        (MapFunction<Row, String>) row -> "Name : " + row.getString(1),
                                        stringEncoder
        );
        teenagersNamesDF.show();

        // or by field name
        Dataset<String> teenagerNamesByFieldDF = teenagers.map(
                                        (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                                        stringEncoder);
        teenagerNamesByFieldDF.show();
    }
    private static void interoperatingWithRDDsProgrammatically() {
        /*
        When JavaBean classes cannot be defined ahead of time (for example, the structure of records is
        encoded in a string, or a text dataset will be parsed and fields will be projected differently
        for different users), a Dataset<Row> can be created programmatically with three steps.

        1. Create an RDD of Rows from the original RDD;
        2. Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
        3. Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.
         */

        // Creates an RDD
        String filePath = Path.of("src", "main", "resources", "people.txt").toString();
        JavaRDD<String> peopleRDD = sparkSession.sparkContext().textFile(filePath, 1).toJavaRDD();

        // The schema is encoded in a string
        String schemaString = "name age";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String field: schemaString.split(" ")) {
            fields.add(DataTypes.createStructField(field, DataTypes.StringType, true));
        }

        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(", ");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        // Apply the schema to the RDD
        Dataset<Row> peopleDataFrame = sparkSession.createDataFrame(rowRDD, schema);

        // Creates a temporary view using the DataFrame
        peopleDataFrame.createOrReplaceTempView("people");

        // SQL can be run over now
        Dataset<Row> results = sparkSession.sql("SELECT * FROM PEOPLE");

        // The results of SQL queries are DataFrames and support all the normal RDD operations
        // The columns of a row in the result can be accessed by field index or by field name

        Dataset<String> nameDs = results.map(
                                        (MapFunction<Row, String>) row -> "Name : " + row.getString(0),
                                        Encoders.STRING()
        );
        nameDs.show();
    }

}
