package software.amazon.qldb.export.impl;

import com.amazon.ion.IonInt;
import com.amazon.ion.IonStruct;
import software.amazon.qldb.export.RevisionVisitor;

import java.util.*;


/**
 * Counts the number of documents in each table and displays the counts to the
 * terminal at the end of processing.  The accounts are actually a net change
 * to table document counts because the export(s) being processed may not
 * represent the entire transaction history of the ledger.
 *
 * Counts are included for dropped tables.  The table ID is provided in the
 * output to distinguish between tables with the same name.  This is possible
 * if a table is dropped and a table with the same name is later created.
 */
public class TableDocumentCountRevisionVisitor implements RevisionVisitor {

    private Map<String, TableDocCount> counts = new HashMap<>();


    /**
     * Retrieve the counts for the tables.
     */
    public List<TableDocCount> getCounts() {
        return new ArrayList<>(counts.values());
    }


    @Override
    public void setup() {
    }

    @Override
    public void visit(IonStruct revision, String tableId, String tableName) {

        IonStruct metadata = (IonStruct) revision.get("metadata");
        int version = ((IonInt) metadata.get("version")).intValue();
        boolean delete = !(revision.containsKey("data") || revision.containsKey("dataHash"));

        if (version > 0 && !delete)
            return;

        TableDocCount ts = counts.get(tableId);
        if (ts == null) {
            ts = new TableDocCount(tableId, tableName);
            counts.put(tableId, ts);
        }

        if (version == 0)  // This is an insert
            ts.add();
        else // Must be a delete
            ts.remove();
    }


    @Override
    public void teardown() {
        List<TableDocCount> structs = new ArrayList<>(counts.values());
        structs.sort(new Comparator<TableDocCount>() {
            @Override
            public int compare(TableDocCount o1, TableDocCount o2) {
                return o1.tableName.equals(o2.tableName) ? o1.tableId.compareTo(o2.tableId) : o1.tableName.compareTo(o2.tableName);
            }
        });

        int maxLen = 0;
        for (TableDocCount tdc : structs) {
            if (tdc.tableName.length() > maxLen)
                maxLen = tdc.tableName.length();
        }

        maxLen += 2;

        int[] widths = {maxLen, 24, 15};
        String spacer = "  ";
        String line = String.format("%-" + widths[0] + "s", "Table Name");
        line += spacer;
        line += String.format("%-" + widths[1] + "s", "Table ID");
        line += spacer;
        line += String.format("%" + widths[2] + "s", "Document Count");
        System.out.println(line);

        line = String.format("%" + widths[0] + "s", "").replace(' ', '-');
        line += spacer;
        line += String.format("%" + widths[1] + "s", "").replace(' ', '-');
        line += spacer;
        line += String.format("%" + widths[2] + "s", "").replace(' ', '-');
        System.out.println(line);

        System.out.println();
        for (TableDocCount ts : structs) {
            line = String.format("%-" + widths[0] + "s", ts.tableName);
            line += spacer;
            line += String.format("%-" + widths[1] + "s", ts.tableId);
            line += spacer;
            line += String.format("%," + widths[2] + "d", ts.documentCount);

            System.out.println(line);
        }
    }


    public static class TableDocCount {
        String tableId;
        String tableName;
        long documentCount = 0L;

        public TableDocCount(String tableId, String tableName) {
            this.tableId = tableId;
            this.tableName = tableName;
        }

        public void add() {
            documentCount++;
        }

        public void remove() {
            documentCount--;
        }
    }
}
