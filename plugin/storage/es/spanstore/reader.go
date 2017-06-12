package spanstore

import (
	"github.com/olivere/elastic"
	"go.uber.org/zap"
	"github.com/uber/jaeger/model"
	"github.com/uber/jaeger/storage/spanstore"
	"errors"
	"github.com/uber/jaeger/pkg/es"
	jModel "github.com/uber/jaeger/model/json"
	"time"
	jConverter "github.com/uber/jaeger/model/converter/json"
	"context"
	"encoding/json"
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
		ctx:	ctx,
		client: client,
		logger: logger,
	}
}

// GetTrace takes a traceID and returns a Trace associated with that traceID
func (s *SpanReader) GetTrace(traceID model.TraceID) (*model.Trace, error) {
	stringTraceID := traceID.String()
	query := elastic.NewTermQuery("traceID", stringTraceID)

	jaegerIndexName := time.Now().Format("2006-01-02")

	searchService, err := elastic.Client.Search(jaegerIndexName).
		Type(spanType).
		Query(query).
		Do(s.ctx)
	if err != nil {
		return err
	}

	esSpansRaw := searchService.Hits.Hits
	spans := make([]*model.Span, len(esSpansRaw))

	for i, esSpanRaw := range esSpansRaw {
		esSpanInByteArray, err := esSpanRaw.Source.MarshalJSON()
		if err != nil {
			return err
		}
		var jsonSpan jModel.Span
		err = json.Unmarshal(esSpanInByteArray, &jsonSpan)
		if err != nil {
			return err
		}
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
func (s *SpanReader) FindTraces(query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	return nil, nil
}