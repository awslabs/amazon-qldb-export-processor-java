# Amazon QLDB Export Processor

This library makes it easy to process and use data in 
[QLDB exports](https://docs.aws.amazon.com/qldb/latest/developerguide/export-journal.html).
It handles the logic of downloading and parsing the export's files from S3 and iterating over the
ledger blocks in the sequential order in which they were committed to the ledger.  As the export processor
encounters ledger blocks and document revisions in the export, it passes those objects into `BlockVisitor` and
`RevisionVisitor` objects to do whatever work is required.  

The framework provides several visitor implementations in the `software.amazon.qldb.export.app` package:

- `ExportPrettyPrintVisitor`:  Simply pretty-prints every block and revision it receives to the console.
- `SequentialLedgerLoadBlockVisitor`:  Loads data from the export into a target ledger sequentially, replicating the 
ACID transactional groupings from the original ledger.  This provides very low transaction throughput into the target
ledger, so it is only suitable for very small exports.  See class Javadoc for more information.
- `SnsLoaderRevisionVisitor`:  Integrates with the [amazon-qldb-ledger-load](https://github.com/awslabs/amazon-qldb-ledger-load-java)
  project via [Amazon SNS](https://aws.amazon.com/sns/) to provide a parallelized ledger data loader utility (see that project for more details).
- `SqsLoaderRevisionVisitor`:  Integrates with the [amazon-qldb-ledger-load](https://github.com/awslabs/amazon-qldb-ledger-load-java)
project via [Amazon SQS](https://aws.amazon.com/sqs/) to provide a parallelized data loader utility (see that project for more details).
- `StatsCalculatorBlockVisitor`: Provides useful statistics about the export, including block and revision counts. Estimates
transaction throughput over the duration covered by the export.  Creates a CSV file with block counts per 1-second timeslice
over the duration of the export, which can be useful for graphing transaction rates over time.
- `TableDocumentCountRevisionVisitor`: Counts the number of active documents per table as of the last block in the export,
displaying the table counts to the console.  Counts include dropped tables.  QLDB is not optimized for table scans, so
this visitor provides a way of getting accurate document table counts from the exports. 

Users can build their own implementations of the `BlockVisitor` or `RevisionVisitor` interfaces to do whatever work they 
need to do with export data.

### Using QLDB Export Processor as a Framework

Clone the repo into your development environment.  Build and install the project locally using Maven with

```bash
mvn install
```

Add this project as a dependency to your project's `pom.xml` file: 

```xml
<dependency>
   <groupId>software.amazon.qldb</groupId>
   <artifactId>amazon-qldb-export-processor</artifactId>
   <version>1.1.0</version>
</dependency>
```

Create implementations of `software.amazon.qldb.export.RevisionVisitor` or `software.amazon.qldb.export.BlockVisitor` 
to perform work with either a single revision's data or an entire block, respectively.

In your application, build a `software.amazon.qldb.export.ExportProcessor`, passing in one or more
visitor objects.  The example below uses a block visitor and a revision visitor.  The block visitor's
`visit()` method will be invoked once for every block in the export set.  The revision visitor's
`visit()` method will be invoked once for every revision in the export set.  Note that blocks
themselves contain revision objects (see the [export output format](https://docs.aws.amazon.com/qldb/latest/developerguide/export-journal.output.html)).
Revision visitors are given the name and ID of the ledger table they belong to, making them slightly
easier to use when consuming revisions than the blocks themselves.

```java
ExportProcessor processor = ExportProcessor.builder()
        .revisionVisitor(new MyAwesomeRevisionVisitor())
        .blockVisitor(new SuperAmazingBlockVisitor())
        .build();
```

The example below uses multiple block visitors.  The export processor will invoke the visitors
in the order they were added to it.

```java
ExportProcessor processor = ExportProcessor.builder()
        .blockVisitor(new DoAThingBlockVisitor())
        .addBlockVisitor(new DoThisOtherThingBlockVisitor())
        .build();
```

You can also pass in a list of visitors, as seen below.

```java
List<BlockVisitor> visitors = new ArrayList<>();
visitors.add(new DoAThingBlockVisitor());
visitors.add(new DoThisOtherThingBlockVisitor());

ExportProcessor processor = ExportProcessor.builder()
        .blockVisitors(visitors)
        .build();
```

To begin processing, call one of the following methods on the `ExportProcessor`:

- `process()`
- `processExport()`
- `processExports()`

The `process()` method accepts a source ledger name and an export ID.  The processor will
call QLDB APIs to get the S3 location of the export files.  The source ledger and export must
both exist in QLDB.

The `processExport()` method accepts an S3 bucket name and S3 path/key to the completed manifest
file.  The processor goes right to S3 to download the export files.  The source ledger does not
need to exist for this method.

The `processExports()` method accepts an S3 bucket name and S3 paths/keys to multiple completed
manifest files.  The processor ensures that all of the exports in the list come from the same
source ledger and contain non-overlapping, contiguous blocks.  This is useful for processing a
set of "incremental" exports.

Here's an example of calling a processing method:

```java

ExportProcessor processor = ExportProcessor.builder()
        .blockVisitor(new DoAThingBlockVisitor())
        .addBlockVisitor(new DoThisOtherThingBlockVisitor())
        .build();

processor.processExport("myBucket", "9BYkcVOzENo6Cf4fdrJI2W");
```

### Using Pre-Built Applications

While this project is intended to be used as an extensible framework, the built-in visitors may be all you
might need.  The `software.amazon.qldb.export.app` package has several runnable classes for
common processing needs and there are Bash shell scripts to run them.

To use, clone the repo into your development environment.  Build the project locally using Maven with:

```bash
mvn package
```

This will create a ZIP file in the project's `target` directory called `amazon-qldb-export-processor-1.1.0.zip`.
Copy that ZIP file to whatever server or compute platform you wish to run it from.  Then:

```bash
$ unzip amazon-qldb-export-processor-1.1.0.zip
$ cd amazon-qldb-export-processor-1.1.0
$ chmod u+x *.sh
```

Now execute the desired script, passing required arguments on the command-line.  For example:

```bash
$ ./export-pretty-print.sh -b myBucket -mp exports/1XTAdzxh1X4KNZlUyBKwR0.2ilSU3AWdLPCuTZig4osfI.completed.manifest
```

## Getting Help

Please use these community resources for getting help.
* Ask a question on StackOverflow and tag it with the [amazon-qldb](https://stackoverflow.com/questions/tagged/amazon-qldb) tag.
* Open a support ticket with [AWS Support](http://docs.aws.amazon.com/awssupport/latest/user/getting-started.html).
* Make a new thread at [AWS QLDB Forum](https://forums.aws.amazon.com/forum.jspa?forumID=353&start=0).
* If you think you may have found a bug or have ideas for new features, please open an issue here in GitHub.

## License

This library is licensed under the MIT-0 license.