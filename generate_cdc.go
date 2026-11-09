package orm

import (
	"fmt"
	"strings"
)

type cdcColumnSpec struct {
	name   string
	fIndex int
	kind   string
	scale  int
}

// buildCDCColumns lists every column with its wire encoding, in SQL row order.
// It walks the field groups in exactly the order buildColOriginInfos and the
// SQL row do, so fIndex lines up with F0..Fn.
func buildCDCColumns(fields *tableFields, fIdx *int) []cdcColumnSpec {
	var cols []cdcColumnSpec
	add := func(i int, kind string, scale int) {
		cols = append(cols, cdcColumnSpec{name: fields.prefix + fields.fields[i].Name, fIndex: *fIdx, kind: kind, scale: scale})
		*fIdx++
	}
	floatKind := func(decimalSize int) string {
		if decimalSize >= 0 {
			return "CDCDecimal"
		}
		return "CDCFloat"
	}
	for _, i := range fields.uIntegers {
		add(i, "CDCNumber", 0)
	}
	for _, i := range fields.references {
		add(i, "CDCNumber", 0)
	}
	for _, i := range fields.referencesMulti {
		add(i, "CDCString", 0)
	}
	for _, i := range fields.integers {
		add(i, "CDCNumber", 0)
	}
	for _, i := range fields.booleans {
		// FakeDelete is a BIGINT holding 0 or the row's own ID.
		if fields.prefix == "" && fields.fields[i].Name == "FakeDelete" {
			add(i, "CDCNumber", 0)
			continue
		}
		add(i, "CDCBool", 0)
	}
	for k, i := range fields.floats {
		add(i, floatKind(fields.floatsDecimalSize[k]), fields.floatsPrecision[k])
	}
	for _, i := range fields.times {
		add(i, "CDCDatetime", 0)
	}
	for _, i := range fields.dates {
		add(i, "CDCDate", 0)
	}
	for _, i := range fields.strings {
		add(i, "CDCString", 0)
	}
	for _, i := range fields.uIntegersNullable {
		add(i, "CDCNumber", 0)
	}
	for _, i := range fields.integersNullable {
		add(i, "CDCNumber", 0)
	}
	for _, i := range fields.stringsEnums {
		add(i, "CDCString", 0)
	}
	for _, i := range fields.bytes {
		add(i, "CDCBytes", 0)
	}
	for _, i := range fields.sliceStringsSets {
		add(i, "CDCString", 0)
	}
	for _, i := range fields.booleansNullable {
		add(i, "CDCBool", 0)
	}
	for k, i := range fields.floatsNullable {
		add(i, floatKind(fields.floatsNullableDecimalSize[k]), fields.floatsNullablePrecision[k])
	}
	for _, i := range fields.timesNullable {
		add(i, "CDCDatetime", 0)
	}
	for _, i := range fields.datesNullable {
		add(i, "CDCDate", 0)
	}
	for _, i := range fields.jsonStructs {
		add(i, "CDCString", 0)
	}
	for _, subFields := range fields.structsFields {
		cols = append(cols, buildCDCColumns(subFields, fIdx)...)
	}
	return cols
}

// generateEntityEventForEntity emits an entity's part of the change feed: its
// row image codec, the change one write makes, and the listener event alias.
func (g *codeGenerator) generateEntityEventForEntity(schema *entitySchema, names *entityNames) {
	if !isOnCDCFeed(schema) {
		return
	}
	g.addImport("encoding/json")

	entity := names.entityName
	columnsVar := names.entityPrivate + "CDCColumns"
	fIdx := 0
	cols := buildCDCColumns(schema.fields, &fIdx)

	g.addLine(fmt.Sprintf("// %sDirtyEvent is one %s row change as a listener receives it. Before and", entity, entity))
	g.addLine("// After are read-only entities: Insert sets only After, Delete only Before.")
	g.addLine(fmt.Sprintf("type %sDirtyEvent = orm.DirtyEvent[%s]", entity, entity))
	g.addLine("")

	g.addLine(fmt.Sprintf("var %s = []orm.CDCColumn{", columnsVar))
	for _, c := range cols {
		if c.kind == "CDCDecimal" {
			g.addLine(fmt.Sprintf("\t{Name: %q, Kind: orm.%s, Scale: %d},", c.name, c.kind, c.scale))
			continue
		}
		g.addLine(fmt.Sprintf("\t{Name: %q, Kind: orm.%s},", c.name, c.kind))
	}
	g.addLine("}")
	g.addLine("")

	fieldRefs := make([]string, len(cols))
	for i, c := range cols {
		fieldRefs[i] = fmt.Sprintf("&r.F%d", c.fIndex)
	}
	g.addLine(fmt.Sprintf("func (r *%s) cdcFields() []any {", names.sqlRowName))
	g.addLine(fmt.Sprintf("\treturn []any{%s}", strings.Join(fieldRefs, ", ")))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) PrivateCDCRow(withChanges bool) map[string]any {", entity))
	g.addLine(fmt.Sprintf("\treturn orm.EncodeCDCRow(%s, e.privateSQLRow(withChanges).cdcFields())", columnsVar))
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) PrivateCDCChange(op orm.DirtyOp) orm.CDCChange {", entity))
	g.addLine("\tswitch op {")
	g.addLine("\tcase orm.DirtyInsert:")
	g.addLine("\t\treturn orm.CDCChange{Op: orm.CDCOpCreate, After: e.PrivateCDCRow(true)}")
	g.addLine("\tcase orm.DirtyUpdate:")
	g.addLine("\t\treturn orm.CDCChange{Op: orm.CDCOpUpdate, Before: e.PrivateCDCRow(false), After: e.PrivateCDCRow(true)}")
	g.addLine("\t}")
	if schema.hasFakeDelete {
		g.addLine("\t// A soft delete is an UPDATE in MySQL, so it is one on the feed too.")
		g.addLine("\tif _, soft := e.databaseBind[\"FakeDelete\"]; soft && !e.deleted {")
		g.addLine("\t\treturn orm.CDCChange{Op: orm.CDCOpUpdate, Before: e.PrivateCDCRow(false), After: e.PrivateCDCRow(true)}")
		g.addLine("\t}")
	}
	g.addLine("\treturn orm.CDCChange{Op: orm.CDCOpDelete, Before: e.PrivateCDCRow(false)}")
	g.addLine("}")
	g.addLine("")

	g.addLine(fmt.Sprintf("func (e *%s) PrivateColumnValue(column string) any {", entity))
	g.addLine("\treturn e.privateGetOriginalColumnValue(column)")
	g.addLine("}")
	g.addLine("")

	decodeFn := "decode" + entity + "CDCRow"
	g.addLine(fmt.Sprintf("func %s(ctx orm.Context, image map[string]json.RawMessage) (*%s, error) {", decodeFn, entity))
	g.addLine(fmt.Sprintf("\trow := &%s{}", names.sqlRowName))
	g.addLine(fmt.Sprintf("\tif err := orm.DecodeCDCRow(%s, row.cdcFields(), image); err != nil {", columnsVar))
	g.addLine("\t\treturn nil, err")
	g.addLine("\t}")
	g.addLine(fmt.Sprintf("\treturn &%s{ctx: ctx, id: row.F0, originDatabaseValues: row, snapshot: true}, nil", entity))
	g.addLine("}")
	g.addLine("")

	defaults := collectRequiredEnumDefaults(schema.fields)
	sqlRowInit := "F0: id"
	for _, d := range defaults {
		sqlRowInit += fmt.Sprintf(", F%d: %q", d.fIndex, d.defaultValue)
	}
	g.addLine("// NewSnapshot builds a read-only entity in memory, filled through the setters,")
	g.addLine("// the way a listener receives one. For tests that hand-build events.")
	g.addLine(fmt.Sprintf("func (p %s) NewSnapshot(id uint64, fill func(e *%s)) *%s {", names.providerNamePrivate, entity, entity))
	g.addLine(fmt.Sprintf("	e := &%s{new: true, id: id, originDatabaseValues: &%s{%s}}", entity, names.sqlRowName, sqlRowInit))
	g.addLine("	if fill != nil {")
	g.addLine("		fill(e)")
	g.addLine("	}")
	g.addLine("	e.new = false")
	g.addLine("	e.snapshot = true")
	g.addLine("	return e")
	g.addLine("}")
	g.addLine("")

	g.addLine("func init() {")
	g.addLine(fmt.Sprintf("\torm.RegisterCDCEntity[%s](%q)", entity, schema.tableName))
	g.addLine("}")
	g.addLine("")
}
