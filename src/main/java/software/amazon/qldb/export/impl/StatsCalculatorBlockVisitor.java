/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package software.amazon.qldb.export.impl;

import com.amazon.ion.*;
import com.amazon.ion.system.IonSystemBuilder;
import software.amazon.qldb.export.BlockVisitor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;


/**
 * Collects and reports the following statistics from a QLDB ledger export:
 *
 * <ul>
 *     <li>Total blocks encountered</li>
 *     <li>Total revisions encountered</li>
 *     <li>Total duration in seconds between first and last blocks</li>
 *     <li>Average transaction throughput measured as blocks per second over the duration of the export</li>
 *     <li>Creates a CSV file showing the number of blocks/transactions per second for each 1-second time slice over the duration of the export</li>
 * </ul>
 */
public class StatsCalculatorBlockVisitor implements BlockVisitor {

    private final IonSystem ionSystem = IonSystemBuilder.standard().build();

    private int startBlock = -1;
    private int endBlock = -1;

    private int totalBlocks = 0;
    private int totalRevisions = 0;

    private IonTimestamp startBlockTime = null;
    private IonTimestamp endBlockTime = null;

    private Calendar lastTimeSlice = null;
    private int timeSliceBlockCnt = 0;

    private File outputFile = null;
    private BufferedWriter writer;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private final SimpleDateFormat timestampParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");


    private StatsCalculatorBlockVisitor(int startBlock, int endBlock, File outputFile) {
        this.startBlock = startBlock;
        this.endBlock = endBlock;
        this.outputFile = outputFile;

        timestampParser.setTimeZone(TimeZone.getTimeZone("UTC"));
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public File getOutputFile() {
        return outputFile;
    }
    public int getStartBlock() {
        return startBlock;
    }
    public int getEndBlock() {
        return endBlock;
    }


    @Override
    public void setup() {
        if (outputFile == null)
            return;

        try {
            if (outputFile.exists())
                outputFile.delete();

            writer = new BufferedWriter(new FileWriter(outputFile));
        } catch (IOException e) {
            throw new RuntimeException("Unable to initialize file for writing", e);
        }
    }

    @Override
    public void teardown() {
        printResults();

        if (writer == null)
            return;

        try {
            if (lastTimeSlice != null) {
                writer.write(sdf.format(lastTimeSlice.getTime()) + ", " + timeSliceBlockCnt);
                writer.newLine();
            }
        } catch (Exception ex) {
        } finally {
            try {
                writer.flush();
                writer.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Override
    public void visit(IonStruct block) {
        if (startBlock > -1 || endBlock > -1) {
            IonStruct blockAddress = (IonStruct) block.get("blockAddress");
            int blockNumber = ((IonInt) blockAddress.get("sequenceNo")).intValue();

            if (startBlock > -1 && blockNumber < startBlock)
                return;

            if (endBlock > -1 && blockNumber > endBlock)
                return;
        }

        IonTimestamp blockTime = null;
        IonValue timestampVal = block.get("blockTimestamp");
        if (timestampVal.getType() == IonType.TIMESTAMP) {
            blockTime = (IonTimestamp) timestampVal;
        } else if (timestampVal.getType() == IonType.STRING) { // Timestamps will be strings for JSON-formatted exports
            try {
                Timestamp ts = Timestamp.forDateZ(timestampParser.parse(((IonString) timestampVal).stringValue()));
                blockTime = ionSystem.newTimestamp(ts);
            } catch (ParseException pe) {
                throw new RuntimeException("Unable to parse block timestamp " + timestampVal.toPrettyString(), pe);
            }
        }

        if (startBlockTime == null)
            startBlockTime = blockTime;

        if (writer != null) {
            Calendar cal = blockTime.timestampValue().calendarValue();
            cal.set(Calendar.MILLISECOND, 0);

            if (lastTimeSlice == null) {
                timeSliceBlockCnt = 1;
                lastTimeSlice = cal;
            } else {
                if (cal.compareTo(lastTimeSlice) == 0) {
                    timeSliceBlockCnt++;
                } else {
                    try {
                        writer.write(sdf.format(lastTimeSlice.getTime()) + ", " + timeSliceBlockCnt);
                        writer.newLine();
                    } catch (IOException ioe) {
                        throw new RuntimeException("Unable to write to output file", ioe);
                    }

                    timeSliceBlockCnt = 1;
                    lastTimeSlice = cal;
                }
            }
        }

        if (block.containsKey("revisions")) {
            IonList revisions = (IonList) block.get("revisions");
            for (IonValue val : revisions) {
                IonStruct rev = (IonStruct) val;
                if (rev.containsKey("blockAddress"))
                    totalRevisions++;
            }
        }

        totalBlocks++;
        endBlockTime = blockTime;
    }

    public void printResults() {
        long t0 = startBlockTime.getMillis();
        long duration = endBlockTime.getMillis() - t0;
        double tps = totalBlocks / (double) (duration / 1000);

        System.out.println("Total blocks:    " + totalBlocks);
        System.out.println("Total revisions: " + totalRevisions);
        System.out.println("Duration (sec):  " + (duration / 1000));
        System.out.println("Avg TPS:         " + String.format("%.2f", tps));
    }

    public static StatsCalculatorBlockVisitorBuilder builder() {
        return StatsCalculatorBlockVisitorBuilder.builder();
    }


    public static class StatsCalculatorBlockVisitorBuilder {
        private int startBlock = -1;
        private int endBlock = -1;
        private File outputFile = null;


        public StatsCalculatorBlockVisitorBuilder startBlock(int startBlock) {
            this.startBlock = startBlock;
            return this;
        }


        public StatsCalculatorBlockVisitorBuilder endBlock(int endBlock) {
            this.endBlock = endBlock;
            return this;
        }

        public StatsCalculatorBlockVisitorBuilder outputFile(File outputFile) {
            this.outputFile = outputFile;
            return this;
        }

        public StatsCalculatorBlockVisitor build() {
            StatsCalculatorBlockVisitor visitor = new StatsCalculatorBlockVisitor(startBlock, endBlock, outputFile);
            return visitor;
        }

        public static StatsCalculatorBlockVisitorBuilder builder() {
            return new StatsCalculatorBlockVisitorBuilder();
        }
    }
}
