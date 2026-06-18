/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import com.google.common.collect.ImmutableMap;
import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.connector.common.DebeziumTaskState;
import io.debezium.openlineage.ConnectorContext;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.openlineage.dataset.DatasetMetadata;
import io.debezium.server.iceberg.GlobalConfig;
import io.debezium.server.iceberg.IcebergUtil;
import io.debezium.server.iceberg.converter.EventConverter;
import io.debezium.server.iceberg.converter.SchemaConverter;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper to perform operations on iceberg tables
 *
 * @author Rafael Acevedo
 */
@Dependent
public class IcebergTableOperator {

  static final ImmutableMap<Operation, Integer> CDC_OPERATION_PRIORITY =
      ImmutableMap.of(
          Operation.INSERT, 1, Operation.READ, 2, Operation.UPDATE, 3, Operation.DELETE, 4);
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableOperator.class);
  @Inject IcebergTableWriterFactory writerFactory;
  @Inject GlobalConfig config;

  private volatile ConnectorContext openLineageContext;

  protected List<EventConverter> deduplicateBatch(List<EventConverter> events) {

    ConcurrentHashMap<Object, EventConverter> deduplicatedEvents = new ConcurrentHashMap<>();

    events.forEach(
        e -> {
          if (!e.hasKeyData()) {
            throw new DebeziumException(
                "Cannot deduplicate data with null key! destination:'"
                    + e.destination()
                    + "' event: '"
                    + e.value().toString()
                    + "'");
          }

          try {
            e.setNewKey(e.cdcOpValue() == Operation.INSERT);
            // deduplicate using key(PK)
            deduplicatedEvents.merge(
                e.key(),
                e,
                (oldValue, newValue) -> {
                  if (this.compareByTsThenOp(oldValue, newValue) <= 0) {
                    return newValue;
                  } else {
                    return oldValue;
                  }
                });
          } catch (Exception ex) {
            throw new DebeziumException("Failed to deduplicate events", ex);
          }
        });

    return new ArrayList<>(deduplicatedEvents.values());
  }

  /**
   * This is used to deduplicate events within given batch.
   *
   * <p>Forex ample a record can be updated multiple times in the source. for example insert
   * followed by update and delete. for this case we need to only pick last change event for the
   * row.
   *
   * <p>Its used when `upsert` feature enabled (when the consumer operating non append mode) which
   * means it should not add duplicate records to target table.
   *
   * @param lhs
   * @param rhs
   * @return
   */
  private int compareByTsThenOp(EventConverter lhs, EventConverter rhs) {
    if (config.iceberg().cdcSourceTsField().orElse("").isBlank()) {
      rhs.setNewKey(lhs.isNewKey());
      return -1;
    }

    int result = Long.compare(lhs.cdcSourceTsValue(), rhs.cdcSourceTsValue());

    if (result == 0) {
      // return (x < y) ? -1 : ((x == y) ? 0 : 1);
      result =
          CDC_OPERATION_PRIORITY
              .getOrDefault(lhs.cdcOpValue(), -1)
              .compareTo(CDC_OPERATION_PRIORITY.getOrDefault(rhs.cdcOpValue(), -1));
    }

    return result;
  }

  /**
   * If given schema contains new fields compared to target table schema then it adds new fields to
   * target iceberg table.
   *
   * <p>Its used when allow field addition feature is enabled.
   *
   * @param icebergTable
   * @param newSchema
   */
  private void applyFieldAddition(Table icebergTable, Schema newSchema) {

    UpdateSchema us =
        icebergTable
            .updateSchema()
            .unionByNameWith(newSchema)
            .setIdentifierFields(newSchema.identifierFieldNames());
    Schema newSchemaCombined = us.apply();

    // @NOTE avoid committing when there is no schema change. commit creates new commit even when
    // there is no change!
    if (!icebergTable.schema().sameSchema(newSchemaCombined)) {
      LOGGER.warn("Extending schema of {}", icebergTable.name());
      us.commit();
    }
  }

  /**
   * Adds list of events to iceberg table.
   *
   * <p>If field addition enabled then it groups list of change events by their schema first. Then
   * adds new fields to iceberg table if there is any. And then follows with adding data to the
   * table.
   *
   * <p>New fields are detected using CDC event schema, since events are grouped by their schemas it
   * uses single event to find-out schema for the whole list of events.
   *
   * @param icebergTable
   * @param events
   */
  public void addToTable(Table icebergTable, List<EventConverter> events) {

    // when operation mode is not upsert deduplicate the events to avoid inserting duplicate row
    if (config.iceberg().upsert() && !icebergTable.schema().identifierFieldIds().isEmpty()) {
      events = deduplicateBatch(events);
    }

    if (!config.iceberg().allowFieldAddition()) {
      // if field additions not enabled add set of events to table
      addToTablePerSchema(icebergTable, events);
    } else {
      Map<SchemaConverter, List<EventConverter>> eventsGroupedBySchema =
          events.stream().collect(Collectors.groupingBy(EventConverter::schemaConverter));
      LOGGER.debug(
          "Batch got {} records with {} different schema!!",
          events.size(),
          eventsGroupedBySchema.keySet().size());

      for (Map.Entry<SchemaConverter, List<EventConverter>> schemaEvents :
          eventsGroupedBySchema.entrySet()) {
        // extend table schema if new fields found
        applyFieldAddition(
            icebergTable,
            schemaEvents
                .getValue()
                .get(0)
                .icebergSchema(config.iceberg().createIdentifierFields()));
        // add set of events to table
        addToTablePerSchema(icebergTable, schemaEvents.getValue());
      }
    }
  }

  /**
   * Adds list of change events to iceberg table. All the events are having same schema.
   *
   * @param icebergTable
   * @param events
   */
  private void addToTablePerSchema(Table icebergTable, List<EventConverter> events) {
    // Initialize a task writer to write data and, when needed, row-level deletes.
    final Schema tableSchema = icebergTable.schema();
    final boolean useDeleteVectors = shouldUseDeleteVectors(icebergTable);
    final Schema identifierSchema =
        TypeUtil.select(tableSchema, tableSchema.identifierFieldIds());
    BaseTaskWriter<Record> writer =
        useDeleteVectors
            ? writerFactory.createAppend(icebergTable)
            : writerFactory.create(icebergTable);
    List<RecordWrapper> recordsToDelete = useDeleteVectors ? new ArrayList<>() : List.of();
    try (writer) {
      for (EventConverter e : events) {
        final RecordWrapper record =
            (config.iceberg().upsert() && !tableSchema.identifierFieldIds().isEmpty())
                ? e.convert(tableSchema)
                : e.convertAsAppend(tableSchema);
        if (useDeleteVectors && requiresDelete(record)) {
          recordsToDelete.add(record);
        }
        if (!useDeleteVectors || shouldWriteRecord(record)) {
          writer.write(record);
        }
      }

      WriteResult files = writer.complete();
      DeleteWriteResult deleteVectors =
          useDeleteVectors
              ? writeDeleteVectors(icebergTable, identifierSchema, recordsToDelete)
              : new DeleteWriteResult(List.of(), CharSequenceSet.empty(), List.of());

      if (files.deleteFiles().length > 0 || !deleteVectors.deleteFiles().isEmpty()) {
        RowDelta newRowDelta = icebergTable.newRowDelta();
        Arrays.stream(files.dataFiles()).forEach(newRowDelta::addRows);
        Arrays.stream(files.deleteFiles()).forEach(newRowDelta::addDeletes);
        deleteVectors.deleteFiles().forEach(newRowDelta::addDeletes);
        newRowDelta.commit();
      } else {
        AppendFiles appendFiles = icebergTable.newAppend();
        Arrays.stream(files.dataFiles()).forEach(appendFiles::appendFile);
        appendFiles.commit();
      }
    } catch (IOException ex) {
      try {
        writer.abort();
      } catch (IOException e) {
        // pass
      }
      throw new DebeziumException(
          "Failed to write data to table:`" + icebergTable.name() + "`", ex);
    }

    LOGGER.info("Committed {} events to table! {}", events.size(), icebergTable.location());

    // Emit OpenLineage output dataset metadata after successful commit
    if (config.iceberg().openlineageEnabled()) {
      try {
        emitOpenLineageEvent(icebergTable);
      } catch (Exception e) {
        LOGGER.debug("OpenLineage emission failed (non-critical)", e);
      }
    }
  }

  private ConnectorContext getOpenLineageContext() {
    if (openLineageContext == null) {
      synchronized (this) {
        if (openLineageContext == null) {
          openLineageContext =
              new ConnectorContext(
                  "debezium-server-iceberg", "iceberg", "0", Module.version(), java.util.Map.of());
        }
      }
    }
    return openLineageContext;
  }

  private void emitOpenLineageEvent(Table icebergTable) {
    List<DatasetMetadata.FieldDefinition> fields =
        icebergTable.schema().columns().stream()
            .map(f -> new DatasetMetadata.FieldDefinition(f.name(), f.type().toString(), ""))
            .toList();

    DatasetMetadata metadata =
        new DatasetMetadata(
            icebergTable.name(),
            DatasetMetadata.DatasetKind.OUTPUT,
            DatasetMetadata.TABLE_DATASET_TYPE,
            DatasetMetadata.DataStore.DATABASE,
            fields);

    DebeziumOpenLineageEmitter.emit(
        getOpenLineageContext(), DebeziumTaskState.RUNNING, List.of(metadata));
  }

  private boolean shouldUseDeleteVectors(Table icebergTable) {
    return config.iceberg().upsert()
        && !icebergTable.schema().identifierFieldIds().isEmpty()
        && Integer.parseInt(icebergTable.properties().getOrDefault("format-version", "2")) >= 3;
  }

  private boolean requiresDelete(RecordWrapper record) {
    return !(record.isNewKey() && !config.iceberg().keepDeletes() && record.op() != Operation.DELETE);
  }

  private boolean shouldWriteRecord(RecordWrapper record) {
    return record.op() != Operation.DELETE || config.iceberg().keepDeletes();
  }

  private DeleteWriteResult writeDeleteVectors(
      Table icebergTable, Schema identifierSchema, List<RecordWrapper> recordsToDelete)
      throws IOException {
    if (recordsToDelete.isEmpty() || icebergTable.currentSnapshot() == null) {
      return new DeleteWriteResult(List.of(), CharSequenceSet.empty(), List.of());
    }

    Expression filter = buildDeleteVectorFilter(identifierSchema, recordsToDelete);
    if (filter == null) {
      return new DeleteWriteResult(List.of(), CharSequenceSet.empty(), List.of());
    }

    Map<String, DataFile> dataFilesByPath = new ConcurrentHashMap<>();
    Map<String, List<DeleteFile>> currentPositionDeletesByPath = new ConcurrentHashMap<>();
    try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().filter(filter).planFiles()) {
      for (FileScanTask task : tasks) {
        String path = task.file().location().toString();
        dataFilesByPath.put(path, task.file());
        List<DeleteFile> currentPositionDeletes = new ArrayList<>();
        task.deletes()
            .forEach(
                deleteFile -> {
                  if (deleteFile.content() == FileContent.POSITION_DELETES) {
                    currentPositionDeletes.add(deleteFile);
                  }
                });
        if (!currentPositionDeletes.isEmpty()) {
          currentPositionDeletesByPath.put(path, currentPositionDeletes);
        }
      }
    }

    if (dataFilesByPath.isEmpty()) {
      return new DeleteWriteResult(List.of(), CharSequenceSet.empty(), List.of());
    }

    Schema scanSchema =
        new Schema(
            Stream.concat(
                    identifierSchema.columns().stream(),
                    Stream.of(MetadataColumns.FILE_PATH, MetadataColumns.SPEC_ID, MetadataColumns.ROW_POSITION))
                .toList());
    DVFileWriter dvWriter =
        new BaseDVFileWriter(
            IcebergUtil.getTableOutputFileFactory(icebergTable, FileFormat.PUFFIN),
            path -> {
              List<DeleteFile> deleteFiles = currentPositionDeletesByPath.get(path);
              if (deleteFiles == null || deleteFiles.isEmpty()) {
                return null;
              }
              return new org.apache.iceberg.data.BaseDeleteLoader(
                      deleteFile -> icebergTable.io().newInputFile(deleteFile))
                  .loadPositionDeletes(deleteFiles, path);
            });

    try (dvWriter;
        CloseableIterable<Record> existingRows =
            IcebergGenerics.read(icebergTable).project(scanSchema).where(filter).build()) {
      for (Record existingRow : existingRows) {
        String path = existingRow.getField(MetadataColumns.FILE_PATH.name()).toString();
        DataFile dataFile = dataFilesByPath.get(path);
        if (dataFile == null) {
          continue;
        }
        Integer specId = (Integer) existingRow.getField(MetadataColumns.SPEC_ID.name());
        Long rowPosition = (Long) existingRow.getField(MetadataColumns.ROW_POSITION.name());
        dvWriter.delete(path, rowPosition, icebergTable.specs().get(specId), dataFile.partition());
      }
    }

    return dvWriter.result();
  }

  private Expression buildDeleteVectorFilter(
      Schema identifierSchema, List<RecordWrapper> recordsToDelete) {
    Expression filter = null;
    for (RecordWrapper record : recordsToDelete) {
      Expression recordFilter = null;
      for (Types.NestedField field : identifierSchema.columns()) {
        Object value = record.getField(field.name());
        Expression fieldFilter =
            value == null ? Expressions.isNull(field.name()) : Expressions.equal(field.name(), value);
        recordFilter = recordFilter == null ? fieldFilter : Expressions.and(recordFilter, fieldFilter);
      }
      if (recordFilter != null) {
        filter = filter == null ? recordFilter : Expressions.or(filter, recordFilter);
      }
    }
    return filter;
  }
}
