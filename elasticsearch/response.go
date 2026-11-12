package elasticsearch

import (
	"encoding/json"
	"strconv"
	"strings"
)

type RawResult struct {
	Hits         RawHits        `json:"hits"`
	Aggregations map[string]any `json:"aggregations,omitempty"`
}

type RawHits struct {
	Total    RawTotal `json:"total"`
	HitsList []RawHit `json:"hits"`
}

type RawTotal struct {
	Value uint64 `json:"value"`
}

type RawHit struct {
	ID     string          `json:"_id"`
	Score  float64         `json:"_score"`
	Source json.RawMessage `json:"_source"`
}

type SortOption struct {
	Field string
	Desc  bool
}

type SearchHit struct {
	ID    uint64
	Score float64
}

type SearchResponse[T any] struct {
	TotalCount uint64
	Hits       []*SearchHit
	Facets     T
}

type TermsFacetBucket struct {
	Key   uint64
	Count uint64
}

type StringTermsFacetBucket struct {
	Key   string
	Count uint64
}

func (r *RawResult) ParseHits() []*SearchHit {
	if r == nil || len(r.Hits.HitsList) == 0 {
		return []*SearchHit{}
	}

	hits := make([]*SearchHit, 0, len(r.Hits.HitsList))
	for i := range r.Hits.HitsList {
		hit := &r.Hits.HitsList[i]

		var source struct {
			ID json.Number `json:"id"`
		}

		if err := json.Unmarshal(hit.Source, &source); err != nil {
			continue
		}

		// Raw digits: snowflake IDs exceed float64's exact range.
		id, err := strconv.ParseUint(source.ID.String(), 10, 64)
		if err != nil {
			continue
		}

		hits = append(hits, &SearchHit{
			ID:    id,
			Score: hit.Score,
		})
	}

	return hits
}

func BuildSearchResponse[T any](result *RawResult, parseFacets func(aggs map[string]any) T) *SearchResponse[T] {
	response := &SearchResponse[T]{
		TotalCount: result.Hits.Total.Value,
		Hits:       result.ParseHits(),
	}

	if parseFacets != nil {
		response.Facets = parseFacets(result.Aggregations)
	}

	return response
}

func SanitizeQuery(query string) string {
	return strings.TrimSpace(query)
}

func ParseMinAgg(aggs map[string]any, field Field) uint64 {
	aggMap, ok := aggs[MinAggName(field)].(map[string]any)
	if !ok {
		return 0
	}

	v, ok := AsUint64(aggMap["value"])
	if !ok {
		return 0
	}

	return v
}

func ParseMaxAgg(aggs map[string]any, field Field) uint64 {
	aggMap, ok := aggs[MaxAggName(field)].(map[string]any)
	if !ok {
		return 0
	}

	v, ok := AsUint64(aggMap["value"])
	if !ok {
		return 0
	}

	return v
}

func ParseFilterAggCount(aggs map[string]any, name string) uint64 {
	aggMap, ok := aggs[name].(map[string]any)
	if !ok {
		return 0
	}

	v, ok := AsUint64(aggMap["doc_count"])
	if !ok {
		return 0
	}

	return v
}

func ParseTermsFacet(aggs map[string]any, field Field) []*TermsFacetBucket {
	aggMap, ok := aggs[AggName(field)].(map[string]any)
	if !ok {
		return nil
	}

	bucketsRaw, ok := aggMap["buckets"].([]any)
	if !ok {
		return nil
	}

	result := make([]*TermsFacetBucket, 0, len(bucketsRaw))
	for _, bucket := range bucketsRaw {
		bucketMap, ok := bucket.(map[string]any)
		if !ok {
			continue
		}

		keyVal, keyOK := AsUint64(bucketMap["key"])

		countVal, countOK := AsUint64(bucketMap["doc_count"])
		if !keyOK || !countOK {
			continue
		}

		result = append(result, &TermsFacetBucket{
			Key:   keyVal,
			Count: countVal,
		})
	}

	return result
}

func ParseStringTermsFacet(aggs map[string]any, field Field) []*StringTermsFacetBucket {
	aggMap, ok := aggs[AggName(field)].(map[string]any)
	if !ok {
		return nil
	}

	return parseStringTermsBuckets(aggMap)
}

func ParseFilterStringTermsAggBuckets(aggs map[string]any, filterName string, subField Field) []*StringTermsFacetBucket {
	filterMap, ok := aggs[filterName].(map[string]any)
	if !ok {
		return nil
	}

	subAggMap, ok := filterMap[AggName(subField)].(map[string]any)
	if !ok {
		return nil
	}

	return parseStringTermsBuckets(subAggMap)
}

func parseStringTermsBuckets(aggMap map[string]any) []*StringTermsFacetBucket {
	bucketsRaw, ok := aggMap["buckets"].([]any)
	if !ok {
		return nil
	}

	result := make([]*StringTermsFacetBucket, 0, len(bucketsRaw))
	for _, bucket := range bucketsRaw {
		bucketMap, ok := bucket.(map[string]any)
		if !ok {
			continue
		}

		keyVal, keyOK := bucketMap["key"].(string)

		countVal, countOK := AsUint64(bucketMap["doc_count"])
		if !keyOK || !countOK {
			continue
		}

		result = append(result, &StringTermsFacetBucket{
			Key:   keyVal,
			Count: countVal,
		})
	}

	return result
}
