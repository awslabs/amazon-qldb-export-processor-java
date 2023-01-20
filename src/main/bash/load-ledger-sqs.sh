#!/bin/bash

CP=amazon-qldb-export-processor-1.1.0.jar
for jar in `ls lib`
do
  CP=$CP:lib/$jar
done

java -cp $CP software.amazon.qldb.export.app.SqsLedgerLoader $@
