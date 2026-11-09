package orm

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// The change feed speaks Debezium's MySQL envelope, value encodings included,
// whoever produces it. The ORM publishes it itself until Debezium takes over the
// same subjects; nothing that reads the feed can tell the two apart.

// CDCProducer decides who writes the change feed.
type CDCProducer uint8

const (
	// CDCProducerORM publishes every write from the ORM after commit.
	CDCProducerORM CDCProducer = iota
	// CDCProducerExternal leaves the feed to Debezium: the ORM publishes nothing
	// and writes no outbox rows.
	CDCProducerExternal
)

// CDC operation codes, as Debezium writes them.
const (
	CDCOpCreate = "c"
	CDCOpUpdate = "u"
	CDCOpDelete = "d"
	CDCOpRead   = "r"
)

// CDCKind is a column's wire encoding.
type CDCKind uint8

const (
	CDCNumber CDCKind = iota + 1
	CDCBool
	CDCFloat
	CDCDecimal
	CDCDatetime
	CDCDate
	CDCString
	CDCBytes
)

// CDCColumn describes one column of a generated row, in SQL row order.
type CDCColumn struct {
	Name  string
	Kind  CDCKind
	Scale int
}

// CDCChange is one row change before it is wrapped in an envelope.
type CDCChange struct {
	Op     string
	Before map[string]any
	After  map[string]any
}

type cdcSource struct {
	Connector string `json:"connector"`
	DB        string `json:"db"`
	Table     string `json:"table"`
	TsMs      int64  `json:"ts_ms"`
}

type cdcEnvelope struct {
	Before map[string]any `json:"before"`
	After  map[string]any `json:"after"`
	Op     string         `json:"op"`
	TsMs   int64          `json:"ts_ms"`
	Source cdcSource      `json:"source"`
}

type cdcWireEnvelope struct {
	Before json.RawMessage `json:"before"`
	After  json.RawMessage `json:"after"`
	Op     string          `json:"op"`
	TsMs   int64           `json:"ts_ms"`
	Source struct {
		TsMs int64 `json:"ts_ms"`
	} `json:"source"`
}

func marshalCDCChange(change CDCChange, db, table string, at time.Time) ([]byte, error) {
	ms := at.UnixMilli()
	return json.Marshal(cdcEnvelope{
		Before: change.Before,
		After:  change.After,
		Op:     change.Op,
		TsMs:   ms,
		Source: cdcSource{Connector: "orm", DB: db, Table: table, TsMs: ms},
	})
}

// EncodeCDCRow turns a generated SQL row into Debezium's row image. fields are
// pointers to the row's F0..Fn, aligned with cols.
func EncodeCDCRow(cols []CDCColumn, fields []any) map[string]any {
	row := make(map[string]any, len(cols))
	for i, col := range cols {
		row[col.Name] = encodeCDCValue(col, fields[i])
	}
	return row
}

func encodeCDCValue(col CDCColumn, field any) any {
	switch v := field.(type) {
	case *uint64:
		return *v
	case *int64:
		return *v
	case *bool:
		return *v
	case *float64:
		return encodeCDCFloat(col, *v)
	case *time.Time:
		return encodeCDCTime(col, *v)
	case *string:
		return encodeCDCString(col, *v)
	case *sql.NullInt64:
		if !v.Valid {
			return nil
		}
		return v.Int64
	case *sql.NullBool:
		if !v.Valid {
			return nil
		}
		return v.Bool
	case *sql.NullFloat64:
		if !v.Valid {
			return nil
		}
		return encodeCDCFloat(col, v.Float64)
	case *sql.NullTime:
		if !v.Valid {
			return nil
		}
		return encodeCDCTime(col, v.Time)
	case *sql.NullString:
		if !v.Valid {
			return nil
		}
		return encodeCDCString(col, v.String)
	}
	panic(fmt.Sprintf("cdc: column %s has unsupported row type %T", col.Name, field))
}

func encodeCDCFloat(col CDCColumn, v float64) any {
	if col.Kind == CDCDecimal {
		return strconv.FormatFloat(v, 'f', col.Scale, 64)
	}
	return v
}

func encodeCDCTime(col CDCColumn, v time.Time) any {
	if col.Kind == CDCDate {
		return v.UTC().Unix() / 86400
	}
	return v.UTC().UnixMilli()
}

func encodeCDCString(col CDCColumn, v string) any {
	if col.Kind == CDCBytes {
		return base64.StdEncoding.EncodeToString([]byte(v))
	}
	return v
}

// DecodeCDCRow fills a generated SQL row from a row image. A column missing
// from the image decodes as its zero value, which is what a column added after
// the event was written means.
func DecodeCDCRow(cols []CDCColumn, fields []any, row map[string]json.RawMessage) error {
	for i, col := range cols {
		if err := decodeCDCValue(col, row[col.Name], fields[i]); err != nil {
			return fmt.Errorf("cdc column %s: %w", col.Name, err)
		}
	}
	return nil
}

func isCDCNull(raw json.RawMessage) bool {
	return len(raw) == 0 || bytes.Equal(raw, []byte("null"))
}

func decodeCDCValue(col CDCColumn, raw json.RawMessage, field any) error {
	null := isCDCNull(raw)
	var err error
	switch v := field.(type) {
	case *uint64:
		*v = 0
		if !null {
			*v, err = strconv.ParseUint(string(raw), 10, 64)
		}
	case *int64:
		*v = 0
		if !null {
			*v, err = strconv.ParseInt(string(raw), 10, 64)
		}
	case *bool:
		*v = false
		if !null {
			*v, err = decodeCDCBool(raw)
		}
	case *float64:
		*v = 0
		if !null {
			*v, err = decodeCDCFloat(raw)
		}
	case *time.Time:
		*v = time.Time{}
		if !null {
			*v, err = decodeCDCTime(col, raw)
		}
	case *string:
		*v = ""
		if !null {
			*v, err = decodeCDCString(col, raw)
		}
	case *sql.NullInt64:
		*v = sql.NullInt64{Valid: !null}
		if !null {
			v.Int64, err = strconv.ParseInt(string(raw), 10, 64)
		}
	case *sql.NullBool:
		*v = sql.NullBool{Valid: !null}
		if !null {
			v.Bool, err = decodeCDCBool(raw)
		}
	case *sql.NullFloat64:
		*v = sql.NullFloat64{Valid: !null}
		if !null {
			v.Float64, err = decodeCDCFloat(raw)
		}
	case *sql.NullTime:
		*v = sql.NullTime{Valid: !null}
		if !null {
			v.Time, err = decodeCDCTime(col, raw)
		}
	case *sql.NullString:
		*v = sql.NullString{Valid: !null}
		if !null {
			v.String, err = decodeCDCString(col, raw)
		}
	default:
		return fmt.Errorf("unsupported row type %T", field)
	}
	return err
}

// decodeCDCBool accepts Debezium's boolean and the 0/1 it sends for
// tinyint(1) without the boolean converter.
func decodeCDCBool(raw json.RawMessage) (bool, error) {
	switch string(raw) {
	case "true", "1":
		return true, nil
	case "false", "0":
		return false, nil
	}
	return false, fmt.Errorf("not a boolean: %s", raw)
}

// decodeCDCFloat accepts a JSON number, and the string Debezium sends for a
// DECIMAL in string mode.
func decodeCDCFloat(raw json.RawMessage) (float64, error) {
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return 0, err
		}
		return strconv.ParseFloat(s, 64)
	}
	return strconv.ParseFloat(string(raw), 64)
}

func decodeCDCTime(col CDCColumn, raw json.RawMessage) (time.Time, error) {
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return time.Time{}, err
		}
		t, err := time.Parse(time.RFC3339Nano, s)
		return t.UTC(), err
	}
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	if col.Kind == CDCDate {
		return time.Unix(n*86400, 0).UTC(), nil
	}
	return time.UnixMilli(n).UTC(), nil
}

func decodeCDCString(col CDCColumn, raw json.RawMessage) (string, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", err
	}
	if col.Kind == CDCBytes {
		b, err := base64.StdEncoding.DecodeString(s)
		return string(b), err
	}
	return s, nil
}

func decodeCDCRowImage(raw json.RawMessage) (map[string]json.RawMessage, error) {
	if isCDCNull(raw) {
		return nil, nil
	}
	row := make(map[string]json.RawMessage)
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil, err
	}
	return row, nil
}

// cdcEventIdentity is the part of an envelope the reader needs: what happened,
// and to which row.
type cdcEventIdentity struct {
	Op     string `json:"op"`
	Before *struct {
		ID json.Number `json:"ID"`
	} `json:"before"`
	After *struct {
		ID json.Number `json:"ID"`
	} `json:"after"`
}

func parseCDCIdentity(data []byte) (op string, id uint64, err error) {
	var ev cdcEventIdentity
	if err := json.Unmarshal(data, &ev); err != nil {
		return "", 0, err
	}
	var raw json.Number
	switch {
	case ev.After != nil:
		raw = ev.After.ID
	case ev.Before != nil:
		raw = ev.Before.ID
	default:
		return ev.Op, 0, fmt.Errorf("cdc event %q carries no row", ev.Op)
	}
	id, err = strconv.ParseUint(string(raw), 10, 64)
	return ev.Op, id, err
}

func cdcOpLabel(data []byte) string {
	var ev struct {
		Op string `json:"op"`
	}
	if json.Unmarshal(data, &ev) != nil {
		return "unknown"
	}
	switch ev.Op {
	case CDCOpCreate:
		return "insert"
	case CDCOpUpdate:
		return "update"
	case CDCOpDelete:
		return "delete"
	case CDCOpRead:
		return "read"
	}
	return "unknown"
}
