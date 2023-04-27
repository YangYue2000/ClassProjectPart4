JAVAC=javac
JAVA=java
CLASSPATH=.:lib/*
OUTDIR=out

SOURCEDIR=src

sources = $(wildcard $(SOURCEDIR)/**/StatusCode.java $(SOURCEDIR)/**/DBConf.java $(SOURCEDIR)/**/models/AlgebraicOperator.java $(SOURCEDIR)/**/models/AttributeType.java $(SOURCEDIR)/**/models/AssignmentOperator.java $(SOURCEDIR)/**/models/AssignmentExpression.java $(SOURCEDIR)/**/models/ComparisonOperator.java $(SOURCEDIR)/**/models/ComparisonPredicate.java $(SOURCEDIR)/**/models/IndexType.java $(SOURCEDIR)/**/models/IndexRecord.java $(SOURCEDIR)/**/models/NonClusteredBPTreeIndexRecord.java $(SOURCEDIR)/**/models/NonClusteredHashIndexRecord.java $(SOURCEDIR)/**/models/Record.java $(SOURCEDIR)/**/models/TableMetadata.java $(SOURCEDIR)/**/fdb/FDBKVPair.java $(SOURCEDIR)/**/fdb/FDBHelper.java $(SOURCEDIR)/**/utils/*.java $(SOURCEDIR)/**/TableMetadataTransformer.java $(SOURCEDIR)/**/TableManager.java $(SOURCEDIR)/**/TableManagerImpl.java $(SOURCEDIR)/**/RecordsTransformer.java $(SOURCEDIR)/**/IndexTransformer.java $(SOURCEDIR)/**/Cursor.java $(SOURCEDIR)/**/Records.java $(SOURCEDIR)/**/RecordsImpl.java $(SOURCEDIR)/**/Indexes.java $(SOURCEDIR)/**/IndexesImpl.java $(SOURCEDIR)/**/Iterator.java $(SOURCEDIR)/**/SelectIterator.java $(SOURCEDIR)/**/JoinIterator.java $(SOURCEDIR)/**/ProjectIterator.java $(SOURCEDIR)/**/RelationalAlgebraOperators.java $(SOURCEDIR)/**/RelationalAlgebraOperatorsImpl.java $(SOURCEDIR)/**/test/*.java)
classes = $(sources:.java=.class)

preparation: clean
	mkdir -p ${OUTDIR}

clean:
	rm -rf ${OUTDIR}

%.class: %.java
	$(JAVAC) -d "$(OUTDIR)" -cp "$(OUTDIR):$(CLASSPATH)" $<

part1Test: preparation $(classes)
	mkdir -p $(OUTDIR)
	$(JAVA) -cp "$(OUTDIR):$(CLASSPATH)" org.junit.runner.JUnitCore CSCI485ClassProject.test.Part1Test

part2Test: preparation $(classes)
	mkdir -p $(OUTDIR)
	$(JAVA) -cp "$(OUTDIR):$(CLASSPATH)" org.junit.runner.JUnitCore CSCI485ClassProject.test.Part2Test

part3Test: preparation $(classes)
	mkdir -p $(OUTDIR)
	$(JAVA) -cp "$(OUTDIR):$(CLASSPATH)" org.junit.runner.JUnitCore CSCI485ClassProject.test.Part3Test

part4Test: preparation $(classes)
	mkdir -p $(OUTDIR)
	$(JAVA) -cp "$(OUTDIR):$(CLASSPATH)" org.junit.runner.JUnitCore CSCI485ClassProject.test.Part4Test

.PHONY: part1Test part2Test part3Test part4Test clean preparation