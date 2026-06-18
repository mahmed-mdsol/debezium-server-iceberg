package io.debezium.server.iceberg.tableoperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import io.debezium.server.iceberg.BaseTest;
import io.debezium.server.iceberg.converter.EventConverter;
import io.debezium.server.iceberg.converter.JsonEventConverter;
import io.debezium.server.iceberg.testresources.CatalogNessie;
import io.debezium.server.iceberg.testresources.S3Minio;
import io.debezium.server.iceberg.testresources.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

@QuarkusTest
@QuarkusTestResource(value = S3Minio.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = CatalogNessie.class, restrictToAnnotatedClass = true)
@TestProfile(IcebergTableOperatorCopyOnWriteTest.CopyOnWriteProfile.class)
@DisabledIfEnvironmentVariable(named = "DEBEZIUM_FORMAT_VALUE", matches = "connect")
class IcebergTableOperatorCopyOnWriteTest extends BaseTest {

  static String testTable = "inventory.test_table_operator_cow";

  public Table createTable(JsonEventConverter sampleEvent) {
    TableIdentifier tableId = consumer.mapDestination(sampleEvent.destination());
    return consumer.loadIcebergTable(tableId, sampleEvent);
  }

  @Test
  void testCopyOnWriteUpsertDoesNotCreateDeleteFiles() throws Exception {
    Table icebergTable =
        createTable(
            eventBuilder
                .destination(testTable)
                .addKeyField("id", 1)
                .addField("data", "before")
                .addField("__op", "c")
                .build());

    List<EventConverter> insertEvents =
        List.of(
            eventBuilder
                .destination(testTable)
                .addKeyField("id", 1)
                .addField("data", "before")
                .addField("__op", "c")
                .build());
    icebergTableOperator.addToTable(icebergTable, insertEvents);

    List<EventConverter> updateEvents =
        List.of(
            eventBuilder
                .destination(testTable)
                .addKeyField("id", 1)
                .addField("data", "after")
                .addField("__op", "u")
                .build());
    icebergTableOperator.addToTable(icebergTable, updateEvents);

    List<String> rows = Lists.newArrayList(getTableDataV2(testTable)).stream().map(Object::toString).toList();
    assertEquals(1, rows.size());
    assertTrue(rows.toString().contains("after"));

    icebergTable.refresh();
    long deleteFiles =
        Lists.newArrayList(icebergTable.snapshots()).stream()
            .flatMap(
                snapshot -> {
                  Iterable<DeleteFile> addedDeleteFiles = snapshot.addedDeleteFiles(icebergTable.io());
                  return Lists.newArrayList(addedDeleteFiles).stream();
                })
            .count();
    assertEquals(0, deleteFiles);
  }

  public static class CopyOnWriteProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.iceberg.upsert", "true");
      config.put("debezium.sink.iceberg.upsert-write-mode", "copy-on-write");
      config.put("debezium.sink.iceberg.upsert-keep-deletes", "true");
      return config;
    }
  }
}
