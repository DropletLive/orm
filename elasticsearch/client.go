package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/prometheus/client_golang/prometheus"
)

var ErrIndexExists = errors.New("elasticsearch index already exists")

const errIndexAlreadyExists = "resource_already_exists_exception"

type Logger interface {
	Infow(msg string, keysAndValues ...any)
}

type Client struct {
	client *elasticsearch.Client
	log    Logger
}

func NewClient(log Logger, uri ...string) (*Client, error) {
	client, err := elasticsearch.New(elasticsearch.WithAddresses(uri...))
	if err != nil {
		return nil, err
	}

	return &Client{
		client: client,
		log:    log,
	}, nil
}

func (c *Client) Close(ctx context.Context) error {
	return c.client.Close(ctx)
}

func (c *Client) IndexExists(ctx context.Context, indexName string) (bool, error) {
	start := time.Now()
	exists, err := c.indexExists(ctx, indexName)
	observe(opIndexExists, indexName, start, outcomeOf(err))

	return exists, err
}

func (c *Client) CreateIndex(ctx context.Context, indexName string, body map[string]any) error {
	start := time.Now()
	err := c.createIndex(ctx, indexName, body)
	observe(opCreateIndex, indexName, start, outcomeOf(err))

	return err
}

func (c *Client) DeleteIndex(ctx context.Context, indexName string) error {
	start := time.Now()
	err := c.deleteIndex(ctx, indexName)
	observe(opDeleteIndex, indexName, start, outcomeOf(err))

	return err
}

type Document struct {
	ID   string
	Body any
}

func (c *Client) IndexDocuments(ctx context.Context, indexName string, docs []Document) error {
	if len(docs) == 0 {
		return nil
	}

	start := time.Now()
	err := c.indexDocuments(ctx, indexName, docs)
	observe(opIndexDocument, indexName, start, outcomeOf(err))
	observeBulkSize(opIndexDocument, indexName, len(docs))

	return err
}

func (c *Client) DeleteDocuments(ctx context.Context, indexName string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	start := time.Now()
	missing, err := c.deleteDocuments(ctx, indexName, ids)
	observeBulkSize(opDeleteDocument, indexName, len(ids))

	if err == nil && missing > 0 {
		observe(opDeleteDocument, indexName, start, outcomeNotFound)

		return nil
	}

	observe(opDeleteDocument, indexName, start, outcomeOf(err))

	return err
}

func (c *Client) Count(ctx context.Context, indexName string) (int, error) {
	start := time.Now()
	count, err := c.count(ctx, indexName)
	observe(opCount, indexName, start, outcomeOf(err))

	return count, err
}

func (c *Client) Search(ctx context.Context, indexName string, query map[string]any) (*RawResult, error) {
	start := time.Now()
	result, err := c.search(ctx, indexName, query)
	observe(opSearch, indexName, start, outcomeOf(err))

	if result != nil {
		esSearchHits.With(prometheus.Labels{esLabelIndex: indexName}).
			Observe(float64(result.Hits.Total.Value))
	}

	return result, err
}

func (c *Client) indexExists(ctx context.Context, indexName string) (bool, error) {
	res, err := c.client.Indices.Exists(
		[]string{indexName},
		c.client.Indices.Exists.WithContext(ctx),
	)
	if err != nil {
		return false, fmt.Errorf("failed to check if index exists: %w", err)
	}
	defer res.Body.Close()

	return res.StatusCode == http.StatusOK, nil
}

func (c *Client) createIndex(ctx context.Context, indexName string, body map[string]any) error {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return fmt.Errorf("failed to encode index body: %w", err)
		}
	}

	res, err := c.client.Indices.Create(
		indexName,
		c.client.Indices.Create.WithBody(&buf),
		c.client.Indices.Create.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		bodyBytes, err := io.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("create index error: status %d (failed to read error body: %w)", res.StatusCode, err)
		}

		var errorResponse map[string]any
		if err := json.Unmarshal(bodyBytes, &errorResponse); err == nil {
			if errorMap, ok := errorResponse["error"].(map[string]any); ok {
				if errorType, ok := errorMap["type"].(string); ok {
					if errorType == errIndexAlreadyExists {
						return ErrIndexExists
					}
				}
			}
		}

		return fmt.Errorf("create index error: status %d, body: %s", res.StatusCode, string(bodyBytes))
	}

	return nil
}

func (c *Client) deleteIndex(ctx context.Context, indexName string) error {
	res, err := c.client.Indices.Delete(
		[]string{indexName},
		c.client.Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to delete index: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil
	}

	if res.IsError() {
		return fmt.Errorf("delete index error: %s", res.String())
	}

	return nil
}

func (c *Client) indexDocuments(ctx context.Context, indexName string, docs []Document) error {
	var body bytes.Buffer
	for _, doc := range docs {
		if err := writeBulkAction(&body, "index", indexName, doc.ID); err != nil {
			return err
		}

		if err := json.NewEncoder(&body).Encode(doc.Body); err != nil {
			return fmt.Errorf("failed to encode doc %s: %w", doc.ID, err)
		}
	}

	_, err := c.bulk(ctx, indexName, &body)

	return err
}

func (c *Client) deleteDocuments(ctx context.Context, indexName string, ids []string) (int, error) {
	var body bytes.Buffer
	for _, id := range ids {
		if err := writeBulkAction(&body, "delete", indexName, id); err != nil {
			return 0, err
		}
	}

	return c.bulk(ctx, indexName, &body)
}

func writeBulkAction(body *bytes.Buffer, action, indexName, id string) error {
	meta := map[string]any{
		action: map[string]string{"_index": indexName, "_id": id},
	}
	if err := json.NewEncoder(body).Encode(meta); err != nil {
		return fmt.Errorf("failed to encode bulk %s action for %s: %w", action, id, err)
	}

	return nil
}

func (c *Client) bulk(ctx context.Context, indexName string, body *bytes.Buffer) (int, error) {
	res, err := c.client.Bulk(
		body,
		c.client.Bulk.WithIndex(indexName),
		c.client.Bulk.WithContext(ctx),
		c.client.Bulk.WithRefresh("true"),
	)
	if err != nil {
		return 0, fmt.Errorf("bulk request failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return 0, fmt.Errorf("bulk error: %s", res.String())
	}

	var parsed struct {
		Items []map[string]struct {
			ID     string `json:"_id"`
			Status int    `json:"status"`
			Error  *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}

	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return 0, fmt.Errorf("bulk decode error: %w", err)
	}

	notFound := 0

	var failures []string

	for _, item := range parsed.Items {
		for action, result := range item {
			if action == "delete" && result.Status == http.StatusNotFound {
				notFound++

				continue
			}

			if result.Error == nil {
				continue
			}

			failures = append(failures, fmt.Sprintf("%s %s: %s (%s)", action, result.ID, result.Error.Reason, result.Error.Type))
		}
	}

	if len(failures) > 0 {
		return notFound, fmt.Errorf("bulk had %d failed items: %s", len(failures), strings.Join(failures, "; "))
	}

	return notFound, nil
}

func (c *Client) count(ctx context.Context, indexName string) (int, error) {
	res, err := c.client.Count(
		c.client.Count.WithContext(ctx),
		c.client.Count.WithIndex(indexName),
	)
	if err != nil {
		return 0, fmt.Errorf("count failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return 0, fmt.Errorf("count error: %s", res.String())
	}

	var result struct {
		Count int `json:"count"`
	}

	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("count decode error: %w", err)
	}

	return result.Count, nil
}

func (c *Client) search(ctx context.Context, indexName string, query map[string]any) (*RawResult, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("failed to encode query: %w", err)
	}

	jsonQuery, _ := json.Marshal(query)
	c.log.Infow("[Elasticsearch] Search", "index", indexName, "query", string(jsonQuery))

	res, err := c.client.Search(
		c.client.Search.WithContext(ctx),
		c.client.Search.WithIndex(indexName),
		c.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, fmt.Errorf("search failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("search error: %s", res.String())
	}

	var result RawResult

	dec := json.NewDecoder(res.Body)
	dec.UseNumber()

	if err := dec.Decode(&result); err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
	}

	return &result, nil
}
