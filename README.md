# Amazon QLDB Export Processor

This library makes it easy to process the blocks and revisions of a [QLDB export](https://docs.aws.amazon.com/qldb/latest/developerguide/export-journal.html).  The
library handles the logic of reading the export's files and iterating of the
blocks in the export in sequence.  Users of the library simply have to plug
instances of `BlockVisitor` or `RevisionVisitor` into the framework to perform
the work they need on the export's data.

The library provides several example implementations in the `software.amazon.qldb.export.app` package,
including an integration with the amazon-qldb-ledger-load ledger load project.

## Setup

Build the project with the following command and add it as a dependency to your project.

```bash
mvnw install
```
