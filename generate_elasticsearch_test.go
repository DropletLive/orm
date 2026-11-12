package orm

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type esSamplePriceDocument struct {
	Currency string `json:"currency"`
	Amount   uint64 `json:"amount"`
}

type esSampleVariantDocument struct {
	ID    uint64 `json:"id"`
	Title string `json:"title" es:"text"`
}

type esSampleDocument struct {
	ID         uint64                     `json:"id"`
	Name       string                     `json:"name" es:"text,analyzer:a,autocomplete:ac,sub:stemmed:st:sts"`
	Status     string                     `json:"status" es:"enum:enums.SampleStatus"`
	Code       string                     `json:"code" es:"keyword"`
	Score      float64                    `json:"score"`
	Count      int                        `json:"count"`
	Active     bool                       `json:"active"`
	CreatedAt  time.Time                  `json:"created_at"`
	Price      *esSamplePriceDocument     `json:"price,omitempty"`
	Variants   []*esSampleVariantDocument `json:"variants" es:"nested"`
	Hidden     string                     `json:"hidden" es:"-"`
	NotIndexed string                     `json:"-"`
}

func TestGenerateElasticsearchDocument(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/app\n"), 0644))
	genDir := filepath.Join(dir, "gen")
	require.NoError(t, os.MkdirAll(filepath.Join(genDir, elasticsearchFieldsPackage), 0755))
	stale := filepath.Join(genDir, elasticsearchFieldsPackage, "stale.go")
	require.NoError(t, os.WriteFile(stale, []byte("package esfields\n"), 0644))

	registry := NewRegistry()
	registry.RegisterElasticsearchDocument(&esSampleDocument{}, esSampleDocument{})
	engine, err := registry.ValidateForCodeGen()
	require.NoError(t, err)
	require.NoError(t, Generate(engine, genDir))

	assert.NoFileExists(t, stale)
	source, err := os.ReadFile(filepath.Join(genDir, elasticsearchFieldsPackage, "es_sample.go"))
	require.NoError(t, err)
	code := squashSpace(string(source))

	assert.Contains(t, code, squashSpace("package esfields"))
	assert.Contains(t, code, squashSpace(`"github.com/DropletLive/orm/elasticsearch"`))
	assert.Contains(t, code, squashSpace(`"example.com/app/gen/enums"`))
	assert.Contains(t, code, squashSpace("var esSampleFields = struct {"))
	assert.Contains(t, code, squashSpace("Status    elasticsearch.EnumField[enums.SampleStatus]"))
	assert.Contains(t, code, squashSpace("Name      elasticsearch.TextField"))
	assert.Contains(t, code, squashSpace("Code      elasticsearch.StringField"))
	assert.Contains(t, code, squashSpace("Score     elasticsearch.FloatField"))
	assert.Contains(t, code, squashSpace("Count     elasticsearch.IntField"))
	assert.Contains(t, code, squashSpace("Active    elasticsearch.BoolField"))
	assert.Contains(t, code, squashSpace("CreatedAt elasticsearch.DateField"))
	assert.Contains(t, code, squashSpace(`Amount: "price.amount",`))
	assert.Contains(t, code, squashSpace(`Title: "variants.title",`))
	assert.NotContains(t, code, squashSpace("Hidden"))
	assert.NotContains(t, code, squashSpace("NotIndexed"))

	assert.Contains(t, code, squashSpace(`"price": map[string]any{
		"properties": map[string]any{`))
	assert.Contains(t, code, squashSpace(`"variants": map[string]any{
		"type": "nested",`))
	assert.Contains(t, code, squashSpace(`"count":      map[string]any{"type": "integer"}`))
	assert.Contains(t, code, squashSpace(`"created_at": map[string]any{"type": "date"}`))
	assert.Contains(t, code, squashSpace(`"autocomplete": map[string]any{
				"type":            "text",
				"analyzer":        "ac",
				"search_analyzer": "a",`))
	assert.Contains(t, code, squashSpace(`"stemmed": map[string]any{
				"type":            "text",
				"analyzer":        "st",
				"search_analyzer": "sts",`))
}

type esBadNameStruct struct {
	ID uint64 `json:"id"`
}

type esBadTagDocument struct {
	ID uint64 `json:"id" es:"txt"`
}

type esBadEnumDocument struct {
	Status string `json:"status" es:"enum:db.Status"`
}

func TestElasticsearchDocumentValidation(t *testing.T) {
	for name, document := range map[string]any{
		"type name must end with Document": esBadNameStruct{},
		"unknown es tag option 'txt'":      esBadTagDocument{},
		"must name a generated enum":       esBadEnumDocument{},
	} {
		registry := NewRegistry()
		registry.RegisterElasticsearchDocument(document)
		_, err := registry.ValidateForCodeGen()
		assert.ErrorContains(t, err, name)
	}
}

func squashSpace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
