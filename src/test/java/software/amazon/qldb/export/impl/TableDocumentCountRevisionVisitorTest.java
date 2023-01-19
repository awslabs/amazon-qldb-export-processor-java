package software.amazon.qldb.export.impl;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonSystemBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


public class TableDocumentCountRevisionVisitorTest {
    private final IonSystem ionSystem = IonSystemBuilder.standard().build();

    @Test
    public void testInsert() {
        IonStruct revision = ionSystem.newEmptyStruct();
        IonStruct data = ionSystem.newEmptyStruct();
        IonStruct metadata = ionSystem.newEmptyStruct();

        revision.put("data", data);
        revision.put("metadata", metadata);

        data.put("field1").newString("value 1");
        data.put("field2").newString("value 2");

        metadata.put("id").newString("8F0TPCmdNQ6JTRpiLj2TmW");
        metadata.put("version").newInt(0);

        TableDocumentCountRevisionVisitor visitor = new TableDocumentCountRevisionVisitor();
        visitor.setup();
        visitor.visit(revision, "0000", "test");

        List<TableDocumentCountRevisionVisitor.TableDocCount> counts = visitor.getCounts();
        assertNotNull(counts);
        assertEquals(1, counts.size());
        assertEquals("0000", counts.get(0).tableId);
        assertEquals("test", counts.get(0).tableName);
        assertEquals(1L, counts.get(0).documentCount);
    }


    @Test
    public void testUpdate() {
        IonStruct revision = ionSystem.newEmptyStruct();
        IonStruct data = ionSystem.newEmptyStruct();
        IonStruct metadata = ionSystem.newEmptyStruct();

        revision.put("data", data);
        revision.put("metadata", metadata);

        data.put("field1").newString("value 1");
        data.put("field2").newString("value 2");

        metadata.put("id").newString("8F0TPCmdNQ6JTRpiLj2TmW");
        metadata.put("version").newInt(1);

        TableDocumentCountRevisionVisitor visitor = new TableDocumentCountRevisionVisitor();
        visitor.setup();
        visitor.visit(revision, "0000", "test");

        List<TableDocumentCountRevisionVisitor.TableDocCount> counts = visitor.getCounts();
        assertNotNull(counts);
        assertEquals(0, counts.size());
    }


    @Test
    public void testDelete() {
        IonStruct revision = ionSystem.newEmptyStruct();
        IonStruct metadata = ionSystem.newEmptyStruct();

        revision.put("metadata", metadata);

        metadata.put("id").newString("8F0TPCmdNQ6JTRpiLj2TmW");
        metadata.put("version").newInt(1);

        TableDocumentCountRevisionVisitor visitor = new TableDocumentCountRevisionVisitor();
        visitor.setup();
        visitor.visit(revision, "0000", "test");

        List<TableDocumentCountRevisionVisitor.TableDocCount> counts = visitor.getCounts();
        assertNotNull(counts);
        assertEquals(1, counts.size());
        assertEquals("0000", counts.get(0).tableId);
        assertEquals("test", counts.get(0).tableName);
        assertEquals(-1L, counts.get(0).documentCount);
    }


    @Test
    public void testRedaction() {
        IonStruct revision = ionSystem.newEmptyStruct();
        IonStruct metadata = ionSystem.newEmptyStruct();

        revision.put("dataHash").newString("....");
        revision.put("metadata", metadata);

        metadata.put("id").newString("8F0TPCmdNQ6JTRpiLj2TmW");
        metadata.put("version").newInt(1);

        TableDocumentCountRevisionVisitor visitor = new TableDocumentCountRevisionVisitor();
        visitor.setup();
        visitor.visit(revision, "0000", "test");

        List<TableDocumentCountRevisionVisitor.TableDocCount> counts = visitor.getCounts();
        assertNotNull(counts);
        assertEquals(0, counts.size());
    }

    @Test
    public void testUpdateWithCounts() {
        IonStruct revision1 = ionSystem.newEmptyStruct();
        IonStruct data1 = ionSystem.newEmptyStruct();
        IonStruct metadata1 = ionSystem.newEmptyStruct();

        revision1.put("data", data1);
        revision1.put("metadata", metadata1);

        data1.put("field1").newString("value 1");
        data1.put("field2").newString("value 2");

        metadata1.put("id").newString("8F0TPCmdNQ6JTRpiLj2TmW");
        metadata1.put("version").newInt(0);

        IonStruct revision2 = ionSystem.newEmptyStruct();
        IonStruct data2 = ionSystem.newEmptyStruct();
        IonStruct metadata2 = ionSystem.newEmptyStruct();

        revision2.put("data", data2);
        revision2.put("metadata", metadata2);

        data2.put("field1").newString("value 1");
        data2.put("field2").newString("updated");

        metadata2.put("id").newString("8F0TPCmdNQ6JTRpiLj2TmW");
        metadata2.put("version").newInt(1);

        TableDocumentCountRevisionVisitor visitor = new TableDocumentCountRevisionVisitor();
        visitor.setup();
        visitor.visit(revision1, "0000", "test");
        visitor.visit(revision2, "0000", "test");

        List<TableDocumentCountRevisionVisitor.TableDocCount> counts = visitor.getCounts();
        assertNotNull(counts);
        assertEquals(1, counts.size());
        assertEquals("0000", counts.get(0).tableId);
        assertEquals("test", counts.get(0).tableName);
        assertEquals(1L, counts.get(0).documentCount);
    }
}
