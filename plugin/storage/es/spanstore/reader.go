// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package spanstore

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/olivere/elastic"
	"github.com/uber/jaeger/model"
	jConverter "github.com/uber/jaeger/model/converter/json"
	jModel "github.com/uber/jaeger/model/json"
	"github.com/uber/jaeger/pkg/es"
	"github.com/uber/jaeger/storage/spanstore"
	"go.uber.org/zap"
	"time"
)

const (
	defaultNumTraces = 100
)

var (
	// ErrServiceNameNotSet occurs when attempting to query with an empty service name
	ErrServiceNameNotSet = errors.New("Service Name must be set")

	// ErrStartTimeMinGreaterThanMax occurs when start time min is above start time max
	ErrStartTimeMinGreaterThanMax = errors.New("Start Time Minimum is above Maximum")

	// ErrDurationMinGreaterThanMax occurs when duration min is above duration max
	ErrDurationMinGreaterThanMax = errors.New("Duration Minimum is above Maximum")

	// ErrMalformedRequestObject occurs when a request object is nil
	ErrMalformedRequestObject = errors.New("Malformed request object")

	// ErrDurationAndTagQueryNotSupported occurs when duration and tags are both set
	ErrDurationAndTagQueryNotSupported = errors.New("Cannot query for duration and tags simultaneously")

	// ErrStartAndEndTimeNotSet occurs when start time and end time are not set
	ErrStartAndEndTimeNotSet = errors.New("Start and End Time must be set")
)

// SpanReader can query for and load traces from ElasticSearch
type SpanReader struct {
	ctx    context.Context
	client es.Client
	logger *zap.Logger
}

// NewSpanReader returns a new SpanReader.
func NewSpanReader(client es.Client, logger *zap.Logger) *SpanReader {
	ctx := context.Background()
	return &SpanReader{
		ctx:    ctx,
		client: client,
		logger: logger,
	}
}

// GetTrace takes a traceID and returns a Trace associated with that traceID
func (s *SpanReader) GetTrace(traceID model.TraceID) (*model.Trace, error) {
	return s.readTrace(traceID.String())
}

func (s *SpanReader) readTrace(traceID string) (*model.Trace, error) {
	query := elastic.NewTermQuery("traceID", traceID)

	esSpansRaw, err := s.executeQuery(query)
	if err != nil {
		return err
	}

	spans := make([]*model.Span, len(esSpansRaw))

	for i, esSpanRaw := range esSpansRaw {
		jsonSpan, err := s.esJSONtoJSONSpanModel(esSpanRaw)
		span, err := jConverter.SpanToDomain(&jsonSpan)
		if err != nil {
			return err
		}
		spans[i] = span
	}

	trace := &model.Trace{}
	trace.Spans = spans
	return trace, nil
}

func (s *SpanReader) esJSONtoJSONSpanModel(esSpanRaw *elastic.SearchHit) (*jModel.Span, error) {
	esSpanInByteArray, err := esSpanRaw.Source.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var jsonSpan *jModel.Span
	err = json.Unmarshal(esSpanInByteArray, jsonSpan)
	if err != nil {
		return nil, err
	}
	return jsonSpan
}

// GetServices returns all services traced by Jaeger, ordered by frequency
func (s *SpanReader) GetServices() ([]string, error) {
	serviceAggregation := elastic.NewTermsAggregation().
		Field("serviceName").
		Size(3000) // Must set to some large number. ES deprecated size omission for aggregating all. https://github.com/elastic/elasticsearch/issues/18838

	jaegerIndexName := time.Now().Format("2006-01-02")

	searchService := elastic.Client.Search(jaegerIndexName). // TODO: do we search multiple indices? how many of the most recent?
									Type(serviceType).
									Size(0). // set to 0 because we don't want actual documents.
									Aggregation("distinct_services", serviceAggregation)

	searchResult, err := searchService.Do(s.ctx)
	if err != nil {
		return nil, err
	}
	bucket, found := searchResult.Aggregations.Terms("distinct_services")
	if !found {
		err = errors.New("Could not find aggregation of services")
		return nil, err
	}
	serviceNamesBucket := bucket.Buckets
	serviceNames := make([]string, len(serviceNamesBucket))

	for i, keyitem := range serviceNamesBucket {
		serviceNames[i] = keyitem.Key
	}
	return serviceNames, nil
}

// GetOperations returns all operations for a specific service traced by Jaeger
func (s *SpanReader) GetOperations(service string) ([]string, error) {
	serviceQuery := elastic.NewTermQuery("serviceName", service)
	serviceFilter := elastic.NewFilterAggregation().Filter(serviceQuery)
	jaegerIndexName := time.Now().Format("2006-01-02")
	searchService := elastic.Client.Search(jaegerIndexName).
		Type(serviceType).
		Size(0).
		Aggregation("distinct_operations", serviceFilter)

	searchResult, err := searchService.Do(s.ctx)
	if err != nil {
		return nil, err
	}
	bucket, found := searchResult.Aggregations.Terms("distinct_operations")
	if !found {
		err = errors.New("Could not find aggregation of operations")
		return nil, err
	}
	operationNamesBucket := bucket.Buckets
	operationNames := make([]string, len(operationNamesBucket))

	for i, keyitem := range operationNamesBucket {
		operationNames[i] = keyitem.Key
	}
	return operationNames, nil
}

func validateQuery(p *spanstore.TraceQueryParameters) error {
	if p == nil {
		return ErrMalformedRequestObject
	}
	if p.ServiceName == "" && len(p.Tags) > 0 {
		return ErrServiceNameNotSet
	}
	if p.StartTimeMin.IsZero() || p.StartTimeMax.IsZero() {
		return ErrStartAndEndTimeNotSet
	}
	if !p.StartTimeMin.IsZero() && !p.StartTimeMax.IsZero() && p.StartTimeMax.Before(p.StartTimeMin) {
		return ErrStartTimeMinGreaterThanMax
	}
	if p.DurationMin != 0 && p.DurationMax != 0 && p.DurationMin > p.DurationMax {
		return ErrDurationMinGreaterThanMax
	}
	if (p.DurationMin != 0 || p.DurationMax != 0) && len(p.Tags) > 0 {
		return ErrDurationAndTagQueryNotSupported
	}
	return nil
}

// FindTraces retrieves traces that match the traceQuery
func (s *SpanReader) FindTraces(traceQuery *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	if err := validateQuery(traceQuery); err != nil {
		return nil, err
	}
	if traceQuery.NumTraces == 0 {
		traceQuery.NumTraces = defaultNumTraces
	}
	uniqueTraceIDs, err := s.findTraceIDs(traceQuery, traceQuery.NumTraces)
	if err != nil {
		return nil, err
	}
	var retMe []*model.Trace
	for traceID := range uniqueTraceIDs {
		if len(retMe) >= traceQuery.NumTraces {
			break
		}
		trace, err := s.readTrace(string(traceID))
		if err != nil {
			s.logger.Error("Failure to read trace", zap.String("trace_id", string(traceID)), zap.Error(err))
			continue
		}
		retMe = append(retMe, trace)
	}
	return retMe, nil
}

func (s *SpanReader) executeQuery(query elastic.Query) ([]*elastic.SearchHit, error) {
	jaegerIndexName := time.Now().Format("2006-01-02")
	searchService, err := elastic.Client.Search(jaegerIndexName).
		Type(spanType).
		Query(query).
		Do(s.ctx)
	if err != nil {
		return nil, err
	}
	return searchService.Hits.Hits, nil
}

func (s *SpanReader) findTraceIDs(traceQuery *spanstore.TraceQueryParameters, numOfTraces int) ([]string, error) {
	// {
	//     "size": 0,
	//     "query": {
	//       "bool": {
	//         "must": [
	//           { "match": { "operationName":   "traceQuery.OperationName"      }},
	//           { "match": { "process.serviceName": "traceQuery.ServiceName" }},
	//           { "range":  { "timestamp": { "gte": traceQuery.StartTimeMin, "lte": traceQuery.StartTimeMax }}},
	//           { "range":  { "duration": { "gte": traceQuery.DurationMin, "lte": traceQuery.DurationMax }}},
	//           { "nested" : {
	//             "path" : "tags",
	//             "query" : {
	//                 "bool" : {
	//                     "must" : [
	//                     { "match" : {"tags.key" : "traceQuery.Tags.key"} },
	//                     { "match" : {"tags.value" : "traceQuery.Tags.value"} }
	//                     ]
	//                 }}}}
	//         ]
	//       }
	//     },
	//     "aggs": { "traceIDs" : { "terms" : {"size":1000,"field": "traceID" }}
	//     }
	// }
	aggregation := elastic.NewTermsAggregation().
		Size(numOfTraces).
		Field("traceID")

	boolQuery := elastic.NewBoolQuery()

	//add duration query
	minDurationMicros := traceQuery.DurationMin.Nanoseconds() / int64(time.Microsecond/time.Nanosecond)
	maxDurationMicros := (time.Hour * 24).Nanoseconds() / int64(time.Microsecond/time.Nanosecond)
	if traceQuery.DurationMax != 0 {
		maxDurationMicros = traceQuery.DurationMax.Nanoseconds() / int64(time.Microsecond/time.Nanosecond)
	}
	durationQuery := elastic.NewRangeQuery("duration").Gte(minDurationMicros).Lte(maxDurationMicros)
	boolQuery.Must(durationQuery)

	//add startTime query
	minStartTimeMicros := traceQuery.StartTimeMin.Nanosecond() / int(time.Microsecond/time.Nanosecond)
	maxStartTimeMicros := int((time.Hour * 24).Nanoseconds() / int64(time.Microsecond/time.Nanosecond))
	if traceQuery.DurationMax != 0 {
		maxDurationMicros = traceQuery.StartTimeMax.Nanosecond() / int(time.Microsecond/time.Nanosecond)
	}
	startTimeQuery := elastic.NewRangeQuery("startTime").Gte(minStartTimeMicros).Lte(maxStartTimeMicros)
	boolQuery.Must(startTimeQuery)

	//add process.serviceName query
	if traceQuery.ServiceName != "" {
		serviceNameQuery := elastic.NewMatchQuery("process.serviceName", traceQuery.ServiceName)
		boolQuery.Must(serviceNameQuery)
	}

	//add operationName query
	if traceQuery.OperationName != "" {
		operationNameQuery := elastic.NewMatchQuery("operationName", traceQuery.OperationName)
		boolQuery.Must(operationNameQuery)
	}

	//add tags query (must be nested) TODO: add log tags query
	for k, v := range traceQuery.Tags {
		keyQuery := elastic.NewMatchQuery("tags.key", k)
		valueQuery := elastic.NewMatchQuery("tags.value", v)
		tagBoolQuery := elastic.NewBoolQuery().Must(keyQuery, valueQuery)
		tagQuery := elastic.NewNestedQuery("tags", tagBoolQuery)
		boolQuery.Must(tagQuery)
	}

	jaegerIndexName := time.Now().Format("2006-01-02")

	searchService := elastic.Client.Search(jaegerIndexName). // TODO: do we search multiple indices? how many of the most recent?
									Type(spanType).
									Size(0). // set to 0 because we don't want actual documents.
									Aggregation("traceIDs", aggregation)

	searchResult, err := searchService.Do(s.ctx)
	if err != nil {
		return nil, err
	}

	bucket, found := searchResult.Aggregations.Terms("traceIDs")
	if !found {
		err = errors.New("Could not find aggregation of services")
		return nil, err
	}

	traceIDBuckets := bucket.Buckets
	traceIDs := make([]string, len(traceIDBuckets))

	for i, keyitem := range traceIDBuckets {
		traceIDs[i] = keyitem.Key
	}
	return traceIDs, nil
}
