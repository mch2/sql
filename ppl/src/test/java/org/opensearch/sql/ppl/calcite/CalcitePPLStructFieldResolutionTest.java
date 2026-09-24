/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.junit.Test;

/**
 * Resolving a dotted path into a column that is a struct (Calcite {@code ROW}) rather than a map.
 *
 * <p>A dotted path used to resolve to a single {@code ITEM} access carrying the whole remainder as
 * the key, which is right for a {@code MAP}: vanilla types an {@code object} as {@code MAP<VARCHAR,
 * ANY>} because it stores objects flattened, so {@code city.location.latitude} genuinely is one key
 * there. Against a {@code ROW} it is not a key at all — {@code SqlItemOperator} looks the dotted
 * name up as one field, finds nothing, and throws {@code AssertionError: Cannot infer type of field
 * ... within ROW type}. Being an {@code Error} it escapes the {@code catch (Exception)} in the
 * resolve loop and surfaces as a 500, so every struct path deeper than one segment was unreachable.
 *
 * <p>The schema here is local to this test because the shared spec schemas carry no struct column:
 * {@code t(id INTEGER, city ROW(name VARCHAR, location ROW(latitude DOUBLE, longitude DOUBLE)),
 * attributes MAP<VARCHAR, VARCHAR>)} gives both shapes side by side, so the ROW cases and the
 * unchanged MAP case are asserted against the same table.
 */
public class CalcitePPLStructFieldResolutionTest extends CalcitePPLAbstractTest {

  public CalcitePPLStructFieldResolutionTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    schema.add("t", new StructTable());
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  /**
   * One level down: {@code city.name} is a field access on the struct, not an ITEM key.
   *
   * <p>The projection is named {@code $f0} rather than the path: a {@code RexFieldAccess} carries
   * no alias of its own so Calcite derives one, whereas the {@code ITEM} form below keeps the
   * dotted name it was resolved under. That is output-column naming, independent of whether the
   * path resolves at all, and is pinned here only to record the shape as it stands.
   */
  @Test
  public void testOneSegmentIntoAStructIsAFieldAccess() {
    RelNode root = getRelNode("source=t | fields city.name");
    verifyLogical(root, "LogicalProject($f0=[$1.name])\n  LogicalTableScan(table=[[scott, t]])\n");
  }

  /**
   * Two levels down, which is the case that failed outright: the remainder has to be descended one
   * segment at a time. Joining it produced {@code ITEM($1, 'location.latitude')} and an
   * AssertionError.
   */
  @Test
  public void testTwoSegmentsIntoAStructDescendEachLevel() {
    RelNode root = getRelNode("source=t | fields city.location.latitude");
    verifyLogical(
        root,
        "LogicalProject($f0=[$1.location.latitude])\n  LogicalTableScan(table=[[scott, t]])\n");
  }

  /**
   * A backtick-quoted path is one part, not several, so the prefix walk has nothing to descend and
   * only matches a column literally named {@code city.location.latitude}. Vanilla flattens objects
   * so such a column really exists there; where the object is a struct the same text means the path
   * into it, so the parts are split and retried — reaching the same field access as the unquoted
   * form.
   */
  @Test
  public void testQuotedDottedPathIsSplitAndRetried() {
    RelNode root = getRelNode("source=t | fields `city.location.latitude`");
    verifyLogical(
        root,
        "LogicalProject($f0=[$1.location.latitude])\n  LogicalTableScan(table=[[scott, t]])\n");
  }

  /**
   * A segment that is not a declared child of the ROW is a plain not-found, not a 500.
   *
   * <p>Descent stops as soon as a segment names no child, and what is left must not become an
   * {@code ITEM} key on the struct: {@code ITEM(<ROW>, 'nonexistent')} is precisely the shape that
   * makes {@code SqlItemOperator} throw {@code AssertionError: Cannot infer type of field ...
   * within ROW type}, and being an {@code Error} it escapes the {@code catch (Exception)} around
   * resolution and surfaces to the user as a 500. Reporting the miss as unresolved instead lets it
   * reach the normal {@code Field [...] not found} path, with the available-field suggestions that
   * go with it.
   */
  @Test
  public void testUndeclaredSegmentInsideAStructIsNotFound() {
    Throwable thrown =
        assertThrows(Throwable.class, () -> getRelNode("source=t | fields city.nonexistent"));
    assertFalse(
        "an undeclared struct segment must not surface as an AssertionError, got: " + thrown,
        thrown instanceof AssertionError);
    assertTrue(
        "expected a not-found naming the field, got: " + thrown,
        String.valueOf(thrown.getMessage()).contains("city.nonexistent"));
  }

  /** A struct path is usable in a predicate, not only a projection. */
  @Test
  public void testStructPathInAPredicate() {
    RelNode root = getRelNode("source=t | where city.location.latitude > 40 | fields id");
    verifyLogical(
        root,
        "LogicalProject(id=[$0])\n"
            + "  LogicalFilter(condition=[>($1.location.latitude, 40)])\n"
            + "    LogicalTableScan(table=[[scott, t]])\n");
  }

  /**
   * A MAP keeps the whole remainder as one ITEM key — the behaviour that must not change, since a
   * vanilla object is stored flattened and one dotted key really is one key there.
   */
  @Test
  public void testMapKeepsTheJoinedItemKey() {
    RelNode root = getRelNode("source=t | fields attributes.region.code");
    verifyLogical(
        root,
        "LogicalProject(attributes.region.code=[ITEM($2, 'region.code')])\n"
            + "  LogicalTableScan(table=[[scott, t]])\n");
  }

  /**
   * A table with both a struct column and a map column, so the two shapes are compared directly.
   */
  private static class StructTable extends AbstractTable {
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
      RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
      RelDataType location =
          typeFactory
              .builder()
              .add("latitude", typeFactory.createTypeWithNullability(doubleType, true))
              .add("longitude", typeFactory.createTypeWithNullability(doubleType, true))
              .build();
      RelDataType city =
          typeFactory
              .builder()
              .add("name", typeFactory.createTypeWithNullability(varchar, true))
              .add("location", typeFactory.createTypeWithNullability(location, true))
              .build();
      return typeFactory
          .builder()
          .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
          .add("city", typeFactory.createTypeWithNullability(city, true))
          .add(
              "attributes",
              typeFactory.createTypeWithNullability(
                  typeFactory.createMapType(varchar, varchar), true))
          .build();
    }
  }
}
