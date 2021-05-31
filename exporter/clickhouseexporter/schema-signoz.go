package clickhouseexporter

import "encoding/json"

type Span struct {
	TraceId            string        `json:"traceId,omitempty"`
	SpanId             string        `json:"spanId,omitempty"`
	ParentSpanId       string        `json:"parentSpanId,omitempty"`
	Name               string        `json:"name,omitempty"`
	DurationNano       uint64        `json:"durationNano,omitempty"`
	StartTimeUnixNano  uint64        `json:"startTimeUnixNano,omitempty"`
	ServiceName        string        `json:"serviceName,omitempty"`
	Kind               int32         `json:"kind,omitempty"`
	References         []OtelSpanRef `json:"references,omitempty"`
	Tags               []string      `json:"tags,omitempty"`
	TagsKeys           []string      `json:"tagsKeys,omitempty"`
	TagsValues         []string      `json:"tagsValues,omitempty"`
	StatusCode         int64         `json:"statusCode,omitempty"`
	ExternalHttpMethod string        `json:"externalHttpMethod,omitempty"`
	ExternalHttpUrl    string        `json:"externalHttpUrl,omitempty"`
	Component          string        `json:"component,omitempty"`
	DBSystem           string        `json:"dbSystem,omitempty"`
	DBName             string        `json:"dbName,omitempty"`
	DBOperation        string        `json:"dbOperation,omitempty"`
	PeerService        string        `json:"peerService,omitempty"`
}

type OtelSpanRef struct {
	TraceId string `json:"traceId,omitempty"`
	SpanId  string `json:"spanId,omitempty"`
	RefType string `json:"refType,omitempty"`
}

func (span *Span) GetReferences() *string {
	value, err := json.Marshal(span.References)
	if err != nil {
		return nil
	}

	referencesString := string(value)
	return &referencesString
}
