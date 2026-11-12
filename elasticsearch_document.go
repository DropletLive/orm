package orm

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

const elasticsearchEnumsPrefix = "enums."

type elasticsearchFieldKind string

const (
	elasticsearchKindDefault elasticsearchFieldKind = ""
	elasticsearchKindNested  elasticsearchFieldKind = "nested"
	elasticsearchKindObject  elasticsearchFieldKind = "object"
	elasticsearchKindText    elasticsearchFieldKind = "text"
	elasticsearchKindKeyword elasticsearchFieldKind = "keyword"
	elasticsearchKindFloat   elasticsearchFieldKind = "float"
	elasticsearchKindEnum    elasticsearchFieldKind = "enum"
)

type elasticsearchDocumentSchema struct {
	name   string
	fields []*elasticsearchField
}

type elasticsearchField struct {
	goName   string
	jsonName string
	tag      elasticsearchTag
	goType   reflect.Type
	children []*elasticsearchField
}

type elasticsearchTag struct {
	skip               bool
	kind               elasticsearchFieldKind
	analyzer           string
	searchAnalyzer     string
	autocomplete       string
	autocompleteSearch string
	enumType           string
	subFields          []elasticsearchSubField
}

type elasticsearchSubField struct {
	name           string
	analyzer       string
	searchAnalyzer string
}

var timeType = reflect.TypeOf(time.Time{})

func (r *registry) RegisterElasticsearchDocument(document ...any) {
	for _, d := range document {
		t := reflect.TypeOf(d)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		r.elasticsearchDocuments = append(r.elasticsearchDocuments, t)
	}
}

func resolveElasticsearchDocuments(r *registry, e *engineImplementation) error {
	schemas := make([]*elasticsearchDocumentSchema, 0, len(r.elasticsearchDocuments))
	names := make(map[string]reflect.Type, len(r.elasticsearchDocuments))
	for _, t := range r.elasticsearchDocuments {
		schema, err := newElasticsearchDocumentSchema(t)
		if err != nil {
			return fmt.Errorf("invalid elasticsearch document '%s': %w", t.String(), err)
		}
		if other, ok := names[schema.name]; ok && other != t {
			return fmt.Errorf("elasticsearch documents '%s' and '%s' share the name %s", other.String(), t.String(), schema.name)
		}
		if _, ok := names[schema.name]; ok {
			continue
		}
		names[schema.name] = t
		schemas = append(schemas, schema)
	}
	sort.Slice(schemas, func(i, j int) bool { return schemas[i].name < schemas[j].name })
	e.registry.elasticsearchDocuments = schemas
	return nil
}

func newElasticsearchDocumentSchema(t reflect.Type) (*elasticsearchDocumentSchema, error) {
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("must be a struct")
	}
	name, ok := strings.CutSuffix(t.Name(), "Document")
	if !ok || name == "" {
		return nil, fmt.Errorf("type name must end with Document")
	}
	fields, err := resolveElasticsearchFields(t)
	if err != nil {
		return nil, err
	}
	return &elasticsearchDocumentSchema{name: name, fields: fields}, nil
}

func resolveElasticsearchFields(t reflect.Type) ([]*elasticsearchField, error) {
	var result []*elasticsearchField
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Anonymous || !f.IsExported() {
			continue
		}
		jsonName := elasticsearchJSONName(f.Tag)
		if jsonName == "" {
			continue
		}
		tag, err := parseElasticsearchTag(f.Tag.Get("es"))
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", f.Name, err)
		}
		if tag.skip {
			continue
		}
		goType := f.Type
		if goType.Kind() == reflect.Slice {
			goType = goType.Elem()
		}
		if goType.Kind() == reflect.Ptr {
			goType = goType.Elem()
		}
		field := &elasticsearchField{goName: f.Name, jsonName: jsonName, tag: tag, goType: goType}
		isStruct := goType.Kind() == reflect.Struct && goType != timeType
		switch {
		case (tag.kind == elasticsearchKindNested || tag.kind == elasticsearchKindObject) && isStruct:
			field.children, err = resolveElasticsearchFields(goType)
		case tag.kind == elasticsearchKindDefault && isStruct:
			field.children, err = resolveElasticsearchFields(goType)
			field.tag.kind = elasticsearchKindObject
		}
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", f.Name, err)
		}
		result = append(result, field)
	}
	return result, nil
}

func elasticsearchJSONName(tag reflect.StructTag) string {
	name, _, _ := strings.Cut(tag.Get("json"), ",")
	if name == "-" {
		return ""
	}
	return name
}

func parseElasticsearchTag(value string) (elasticsearchTag, error) {
	if value == "-" {
		return elasticsearchTag{skip: true}, nil
	}
	tag := elasticsearchTag{}
	if value == "" {
		return tag, nil
	}
	for part := range strings.SplitSeq(value, ",") {
		part = strings.TrimSpace(part)
		switch kind := elasticsearchFieldKind(part); kind {
		case elasticsearchKindNested, elasticsearchKindObject, elasticsearchKindText, elasticsearchKindKeyword, elasticsearchKindFloat:
			tag.kind = kind
			continue
		}
		key, option, ok := strings.Cut(part, ":")
		if !ok {
			return tag, fmt.Errorf("unknown es tag option '%s'", part)
		}
		switch key {
		case "analyzer":
			tag.analyzer = option
		case "search_analyzer":
			tag.searchAnalyzer = option
		case "autocomplete":
			tag.autocomplete = option
		case "ac_search":
			tag.autocompleteSearch = option
		case "enum":
			if !strings.HasPrefix(option, elasticsearchEnumsPrefix) {
				return tag, fmt.Errorf("enum '%s' must name a generated enum as %sName", option, elasticsearchEnumsPrefix)
			}
			tag.kind = elasticsearchKindEnum
			tag.enumType = option
		case "sub":
			sub := parseElasticsearchSubField(option)
			if sub.name != "" {
				tag.subFields = append(tag.subFields, sub)
			}
		default:
			return tag, fmt.Errorf("unknown es tag option '%s'", part)
		}
	}
	return tag, nil
}

func parseElasticsearchSubField(value string) elasticsearchSubField {
	parts := strings.SplitN(value, ":", 3)
	sub := elasticsearchSubField{name: parts[0]}
	if len(parts) >= 2 {
		sub.analyzer = parts[1]
	}
	if len(parts) >= 3 {
		sub.searchAnalyzer = parts[2]
	}
	return sub
}
