// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: opentelemetry/proto/trace/v1/trace_config.proto

package v1

import (
	encoding_binary "encoding/binary"
	fmt "fmt"
	io "io"
	math "math"
	math_bits "math/bits"

	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/gogo/protobuf/proto"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

// How spans should be sampled:
// - Always off
// - Always on
// - Always follow the parent Span's decision (off if no parent).
type ConstantSampler_ConstantDecision int32

const (
	ConstantSampler_ALWAYS_OFF    ConstantSampler_ConstantDecision = 0
	ConstantSampler_ALWAYS_ON     ConstantSampler_ConstantDecision = 1
	ConstantSampler_ALWAYS_PARENT ConstantSampler_ConstantDecision = 2
)

var ConstantSampler_ConstantDecision_name = map[int32]string{
	0: "ALWAYS_OFF",
	1: "ALWAYS_ON",
	2: "ALWAYS_PARENT",
}

var ConstantSampler_ConstantDecision_value = map[string]int32{
	"ALWAYS_OFF":    0,
	"ALWAYS_ON":     1,
	"ALWAYS_PARENT": 2,
}

func (x ConstantSampler_ConstantDecision) String() string {
	return proto.EnumName(ConstantSampler_ConstantDecision_name, int32(x))
}

func (ConstantSampler_ConstantDecision) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_5936aa8fa6443e6f, []int{1, 0}
}

// Global configuration of the trace service. All fields must be specified, or
// the default (zero) values will be used for each type.
type TraceConfig struct {
	// The global default sampler used to make decisions on span sampling.
	//
	// Types that are valid to be assigned to Sampler:
	//	*TraceConfig_ConstantSampler
	//	*TraceConfig_TraceIdRatioBased
	//	*TraceConfig_RateLimitingSampler
	Sampler isTraceConfig_Sampler `protobuf_oneof:"sampler"`
	// The global default max number of attributes per span.
	MaxNumberOfAttributes int64 `protobuf:"varint,4,opt,name=max_number_of_attributes,json=maxNumberOfAttributes,proto3" json:"max_number_of_attributes,omitempty"`
	// The global default max number of annotation events per span.
	MaxNumberOfTimedEvents int64 `protobuf:"varint,5,opt,name=max_number_of_timed_events,json=maxNumberOfTimedEvents,proto3" json:"max_number_of_timed_events,omitempty"`
	// The global default max number of attributes per timed event.
	MaxNumberOfAttributesPerTimedEvent int64 `protobuf:"varint,6,opt,name=max_number_of_attributes_per_timed_event,json=maxNumberOfAttributesPerTimedEvent,proto3" json:"max_number_of_attributes_per_timed_event,omitempty"`
	// The global default max number of link entries per span.
	MaxNumberOfLinks int64 `protobuf:"varint,7,opt,name=max_number_of_links,json=maxNumberOfLinks,proto3" json:"max_number_of_links,omitempty"`
	// The global default max number of attributes per span.
	MaxNumberOfAttributesPerLink int64 `protobuf:"varint,8,opt,name=max_number_of_attributes_per_link,json=maxNumberOfAttributesPerLink,proto3" json:"max_number_of_attributes_per_link,omitempty"`
}

func (m *TraceConfig) Reset()         { *m = TraceConfig{} }
func (m *TraceConfig) String() string { return proto.CompactTextString(m) }
func (*TraceConfig) ProtoMessage()    {}
func (*TraceConfig) Descriptor() ([]byte, []int) {
	return fileDescriptor_5936aa8fa6443e6f, []int{0}
}
func (m *TraceConfig) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *TraceConfig) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_TraceConfig.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *TraceConfig) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TraceConfig.Merge(m, src)
}
func (m *TraceConfig) XXX_Size() int {
	return m.Size()
}
func (m *TraceConfig) XXX_DiscardUnknown() {
	xxx_messageInfo_TraceConfig.DiscardUnknown(m)
}

var xxx_messageInfo_TraceConfig proto.InternalMessageInfo

type isTraceConfig_Sampler interface {
	isTraceConfig_Sampler()
	MarshalTo([]byte) (int, error)
	Size() int
}

type TraceConfig_ConstantSampler struct {
	ConstantSampler *ConstantSampler `protobuf:"bytes,1,opt,name=constant_sampler,json=constantSampler,proto3,oneof" json:"constant_sampler,omitempty"`
}
type TraceConfig_TraceIdRatioBased struct {
	TraceIdRatioBased *TraceIdRatioBased `protobuf:"bytes,2,opt,name=trace_id_ratio_based,json=traceIdRatioBased,proto3,oneof" json:"trace_id_ratio_based,omitempty"`
}
type TraceConfig_RateLimitingSampler struct {
	RateLimitingSampler *RateLimitingSampler `protobuf:"bytes,3,opt,name=rate_limiting_sampler,json=rateLimitingSampler,proto3,oneof" json:"rate_limiting_sampler,omitempty"`
}

func (*TraceConfig_ConstantSampler) isTraceConfig_Sampler()     {}
func (*TraceConfig_TraceIdRatioBased) isTraceConfig_Sampler()   {}
func (*TraceConfig_RateLimitingSampler) isTraceConfig_Sampler() {}

func (m *TraceConfig) GetSampler() isTraceConfig_Sampler {
	if m != nil {
		return m.Sampler
	}
	return nil
}

func (m *TraceConfig) GetConstantSampler() *ConstantSampler {
	if x, ok := m.GetSampler().(*TraceConfig_ConstantSampler); ok {
		return x.ConstantSampler
	}
	return nil
}

func (m *TraceConfig) GetTraceIdRatioBased() *TraceIdRatioBased {
	if x, ok := m.GetSampler().(*TraceConfig_TraceIdRatioBased); ok {
		return x.TraceIdRatioBased
	}
	return nil
}

func (m *TraceConfig) GetRateLimitingSampler() *RateLimitingSampler {
	if x, ok := m.GetSampler().(*TraceConfig_RateLimitingSampler); ok {
		return x.RateLimitingSampler
	}
	return nil
}

func (m *TraceConfig) GetMaxNumberOfAttributes() int64 {
	if m != nil {
		return m.MaxNumberOfAttributes
	}
	return 0
}

func (m *TraceConfig) GetMaxNumberOfTimedEvents() int64 {
	if m != nil {
		return m.MaxNumberOfTimedEvents
	}
	return 0
}

func (m *TraceConfig) GetMaxNumberOfAttributesPerTimedEvent() int64 {
	if m != nil {
		return m.MaxNumberOfAttributesPerTimedEvent
	}
	return 0
}

func (m *TraceConfig) GetMaxNumberOfLinks() int64 {
	if m != nil {
		return m.MaxNumberOfLinks
	}
	return 0
}

func (m *TraceConfig) GetMaxNumberOfAttributesPerLink() int64 {
	if m != nil {
		return m.MaxNumberOfAttributesPerLink
	}
	return 0
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*TraceConfig) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*TraceConfig_ConstantSampler)(nil),
		(*TraceConfig_TraceIdRatioBased)(nil),
		(*TraceConfig_RateLimitingSampler)(nil),
	}
}

// Sampler that always makes a constant decision on span sampling.
type ConstantSampler struct {
	Decision ConstantSampler_ConstantDecision `protobuf:"varint,1,opt,name=decision,proto3,enum=opentelemetry.proto.trace.v1.ConstantSampler_ConstantDecision" json:"decision,omitempty"`
}

func (m *ConstantSampler) Reset()         { *m = ConstantSampler{} }
func (m *ConstantSampler) String() string { return proto.CompactTextString(m) }
func (*ConstantSampler) ProtoMessage()    {}
func (*ConstantSampler) Descriptor() ([]byte, []int) {
	return fileDescriptor_5936aa8fa6443e6f, []int{1}
}
func (m *ConstantSampler) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *ConstantSampler) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_ConstantSampler.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *ConstantSampler) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ConstantSampler.Merge(m, src)
}
func (m *ConstantSampler) XXX_Size() int {
	return m.Size()
}
func (m *ConstantSampler) XXX_DiscardUnknown() {
	xxx_messageInfo_ConstantSampler.DiscardUnknown(m)
}

var xxx_messageInfo_ConstantSampler proto.InternalMessageInfo

func (m *ConstantSampler) GetDecision() ConstantSampler_ConstantDecision {
	if m != nil {
		return m.Decision
	}
	return ConstantSampler_ALWAYS_OFF
}

// Sampler that tries to uniformly sample traces with a given ratio.
// The ratio of sampling a trace is equal to that of the specified ratio.
type TraceIdRatioBased struct {
	// The desired ratio of sampling. Must be within [0.0, 1.0].
	SamplingRatio float64 `protobuf:"fixed64,1,opt,name=samplingRatio,proto3" json:"samplingRatio,omitempty"`
}

func (m *TraceIdRatioBased) Reset()         { *m = TraceIdRatioBased{} }
func (m *TraceIdRatioBased) String() string { return proto.CompactTextString(m) }
func (*TraceIdRatioBased) ProtoMessage()    {}
func (*TraceIdRatioBased) Descriptor() ([]byte, []int) {
	return fileDescriptor_5936aa8fa6443e6f, []int{2}
}
func (m *TraceIdRatioBased) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *TraceIdRatioBased) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_TraceIdRatioBased.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *TraceIdRatioBased) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TraceIdRatioBased.Merge(m, src)
}
func (m *TraceIdRatioBased) XXX_Size() int {
	return m.Size()
}
func (m *TraceIdRatioBased) XXX_DiscardUnknown() {
	xxx_messageInfo_TraceIdRatioBased.DiscardUnknown(m)
}

var xxx_messageInfo_TraceIdRatioBased proto.InternalMessageInfo

func (m *TraceIdRatioBased) GetSamplingRatio() float64 {
	if m != nil {
		return m.SamplingRatio
	}
	return 0
}

// Sampler that tries to sample with a rate per time window.
type RateLimitingSampler struct {
	// Rate per second.
	Qps int64 `protobuf:"varint,1,opt,name=qps,proto3" json:"qps,omitempty"`
}

func (m *RateLimitingSampler) Reset()         { *m = RateLimitingSampler{} }
func (m *RateLimitingSampler) String() string { return proto.CompactTextString(m) }
func (*RateLimitingSampler) ProtoMessage()    {}
func (*RateLimitingSampler) Descriptor() ([]byte, []int) {
	return fileDescriptor_5936aa8fa6443e6f, []int{3}
}
func (m *RateLimitingSampler) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *RateLimitingSampler) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_RateLimitingSampler.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *RateLimitingSampler) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RateLimitingSampler.Merge(m, src)
}
func (m *RateLimitingSampler) XXX_Size() int {
	return m.Size()
}
func (m *RateLimitingSampler) XXX_DiscardUnknown() {
	xxx_messageInfo_RateLimitingSampler.DiscardUnknown(m)
}

var xxx_messageInfo_RateLimitingSampler proto.InternalMessageInfo

func (m *RateLimitingSampler) GetQps() int64 {
	if m != nil {
		return m.Qps
	}
	return 0
}

func init() {
	proto.RegisterEnum("opentelemetry.proto.trace.v1.ConstantSampler_ConstantDecision", ConstantSampler_ConstantDecision_name, ConstantSampler_ConstantDecision_value)
	proto.RegisterType((*TraceConfig)(nil), "opentelemetry.proto.trace.v1.TraceConfig")
	proto.RegisterType((*ConstantSampler)(nil), "opentelemetry.proto.trace.v1.ConstantSampler")
	proto.RegisterType((*TraceIdRatioBased)(nil), "opentelemetry.proto.trace.v1.TraceIdRatioBased")
	proto.RegisterType((*RateLimitingSampler)(nil), "opentelemetry.proto.trace.v1.RateLimitingSampler")
}

func init() {
	proto.RegisterFile("opentelemetry/proto/trace/v1/trace_config.proto", fileDescriptor_5936aa8fa6443e6f)
}

var fileDescriptor_5936aa8fa6443e6f = []byte{
	// 560 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x94, 0x94, 0x4f, 0x6f, 0xda, 0x30,
	0x18, 0xc6, 0x93, 0xb2, 0xfe, 0x7b, 0xab, 0xb6, 0xc1, 0xb4, 0x53, 0x54, 0x55, 0x59, 0x17, 0x4d,
	0x1a, 0x97, 0x12, 0xd1, 0x1d, 0xa6, 0xed, 0x30, 0x09, 0xfa, 0x77, 0x12, 0xa2, 0x28, 0x45, 0x9a,
	0xc6, 0xc5, 0x32, 0x89, 0x89, 0xac, 0x25, 0x36, 0x73, 0x5c, 0xd4, 0xdd, 0xf7, 0x01, 0x76, 0xda,
	0x17, 0xd9, 0x97, 0xd8, 0xb1, 0xc7, 0x1d, 0x27, 0xf8, 0x22, 0x53, 0x1c, 0x4a, 0x81, 0xb6, 0x48,
	0xbb, 0xd9, 0xcf, 0xe3, 0xe7, 0xf7, 0xbe, 0x86, 0x37, 0x06, 0x4f, 0xf4, 0x29, 0x57, 0x34, 0xa6,
	0x09, 0x55, 0xf2, 0x9b, 0xd7, 0x97, 0x42, 0x09, 0x4f, 0x49, 0x12, 0x50, 0x6f, 0x50, 0xcd, 0x17,
	0x38, 0x10, 0xbc, 0xc7, 0xa2, 0x8a, 0xf6, 0xd0, 0xfe, 0x4c, 0x20, 0x17, 0x2b, 0xfa, 0x5c, 0x65,
	0x50, 0xdd, 0xdb, 0x89, 0x44, 0x24, 0x72, 0x48, 0xb6, 0xca, 0x6d, 0xf7, 0xfb, 0x32, 0x6c, 0xb4,
	0xb3, 0x23, 0xc7, 0x9a, 0x84, 0x3a, 0x60, 0x05, 0x82, 0xa7, 0x8a, 0x70, 0x85, 0x53, 0x92, 0xf4,
	0x63, 0x2a, 0x6d, 0xf3, 0xc0, 0x2c, 0x6f, 0x1c, 0x1d, 0x56, 0x16, 0xe1, 0x2b, 0xc7, 0xe3, 0xd4,
	0x55, 0x1e, 0xba, 0x30, 0xfc, 0xed, 0x60, 0x56, 0x42, 0x5d, 0xd8, 0xc9, 0xbb, 0x66, 0x21, 0x96,
	0x44, 0x31, 0x81, 0xbb, 0x24, 0xa5, 0xa1, 0xbd, 0xa4, 0xf9, 0xde, 0x62, 0xbe, 0x6e, 0xf2, 0x63,
	0xe8, 0x67, 0xb9, 0x7a, 0x16, 0xbb, 0x30, 0xfc, 0xa2, 0x9a, 0x17, 0x51, 0x04, 0xbb, 0x92, 0x28,
	0x8a, 0x63, 0x96, 0x30, 0xc5, 0x78, 0x34, 0xb9, 0x44, 0x41, 0x17, 0xa9, 0x2e, 0x2e, 0xe2, 0x13,
	0x45, 0x1b, 0xe3, 0xe4, 0xfd, 0x45, 0x4a, 0xf2, 0xa1, 0x8c, 0xde, 0x82, 0x9d, 0x90, 0x1b, 0xcc,
	0xaf, 0x93, 0x2e, 0x95, 0x58, 0xf4, 0x30, 0x51, 0x4a, 0xb2, 0xee, 0xb5, 0xa2, 0xa9, 0xfd, 0xec,
	0xc0, 0x2c, 0x17, 0xfc, 0xdd, 0x84, 0xdc, 0x34, 0xb5, 0x7d, 0xd9, 0xab, 0x4d, 0x4c, 0xf4, 0x1e,
	0xf6, 0x66, 0x83, 0x8a, 0x25, 0x34, 0xc4, 0x74, 0x40, 0xb9, 0x4a, 0xed, 0x65, 0x1d, 0x7d, 0x3e,
	0x15, 0x6d, 0x67, 0xf6, 0xa9, 0x76, 0x51, 0x1b, 0xca, 0x4f, 0x15, 0xc5, 0x7d, 0x2a, 0xa7, 0x51,
	0xf6, 0x8a, 0x26, 0xb9, 0x8f, 0x36, 0xd1, 0xa2, 0xf2, 0x1e, 0x8b, 0x0e, 0xa1, 0x34, 0x4b, 0x8d,
	0x19, 0xff, 0x92, 0xda, 0xab, 0x1a, 0x60, 0x4d, 0x01, 0x1a, 0x99, 0x8e, 0xce, 0xe1, 0xe5, 0xc2,
	0x26, 0xb2, 0xb4, 0xbd, 0xa6, 0xc3, 0xfb, 0x4f, 0x55, 0xcf, 0x48, 0xf5, 0x75, 0x58, 0x1d, 0xff,
	0x3b, 0xee, 0x2f, 0x13, 0xb6, 0xe7, 0x26, 0x08, 0x75, 0x60, 0x2d, 0xa4, 0x01, 0x4b, 0x99, 0xe0,
	0x7a, 0x04, 0xb7, 0x8e, 0x3e, 0xfc, 0xd7, 0x08, 0x4e, 0xf6, 0x27, 0x63, 0x8a, 0x3f, 0xe1, 0xb9,
	0x27, 0x60, 0xcd, 0xbb, 0x68, 0x0b, 0xa0, 0xd6, 0xf8, 0x54, 0xfb, 0x7c, 0x85, 0x2f, 0xcf, 0xce,
	0x2c, 0x03, 0x6d, 0xc2, 0xfa, 0xdd, 0xbe, 0x69, 0x99, 0xa8, 0x08, 0x9b, 0xe3, 0x6d, 0xab, 0xe6,
	0x9f, 0x36, 0xdb, 0xd6, 0x92, 0xfb, 0x0e, 0x8a, 0x0f, 0xc6, 0x12, 0xbd, 0x82, 0x4d, 0x7d, 0x2b,
	0xc6, 0x23, 0xad, 0xea, 0xde, 0x4d, 0x7f, 0x56, 0x74, 0x5f, 0x43, 0xe9, 0x91, 0x61, 0x43, 0x16,
	0x14, 0xbe, 0xf6, 0x53, 0x1d, 0x29, 0xf8, 0xd9, 0xb2, 0xfe, 0xd3, 0xfc, 0x3d, 0x74, 0xcc, 0xdb,
	0xa1, 0x63, 0xfe, 0x1d, 0x3a, 0xe6, 0x8f, 0x91, 0x63, 0xdc, 0x8e, 0x1c, 0xe3, 0xcf, 0xc8, 0x31,
	0xe0, 0x05, 0x13, 0x0b, 0x7f, 0x90, 0xba, 0x35, 0xf5, 0x65, 0xb7, 0x32, 0xab, 0x65, 0x76, 0xce,
	0xa3, 0xf9, 0x10, 0x13, 0x5e, 0x20, 0xe2, 0x98, 0x06, 0x4a, 0x48, 0x8f, 0x71, 0x45, 0x25, 0x27,
	0xb1, 0x17, 0x12, 0x45, 0xf2, 0x37, 0x27, 0xa2, 0x7c, 0xea, 0xc0, 0xdd, 0x03, 0xd4, 0x5d, 0xd1,
	0xe6, 0x9b, 0x7f, 0x01, 0x00, 0x00, 0xff, 0xff, 0xac, 0x76, 0xce, 0xc4, 0xa7, 0x04, 0x00, 0x00,
}

func (m *TraceConfig) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *TraceConfig) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *TraceConfig) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.MaxNumberOfAttributesPerLink != 0 {
		i = encodeVarintTraceConfig(dAtA, i, uint64(m.MaxNumberOfAttributesPerLink))
		i--
		dAtA[i] = 0x40
	}
	if m.MaxNumberOfLinks != 0 {
		i = encodeVarintTraceConfig(dAtA, i, uint64(m.MaxNumberOfLinks))
		i--
		dAtA[i] = 0x38
	}
	if m.MaxNumberOfAttributesPerTimedEvent != 0 {
		i = encodeVarintTraceConfig(dAtA, i, uint64(m.MaxNumberOfAttributesPerTimedEvent))
		i--
		dAtA[i] = 0x30
	}
	if m.MaxNumberOfTimedEvents != 0 {
		i = encodeVarintTraceConfig(dAtA, i, uint64(m.MaxNumberOfTimedEvents))
		i--
		dAtA[i] = 0x28
	}
	if m.MaxNumberOfAttributes != 0 {
		i = encodeVarintTraceConfig(dAtA, i, uint64(m.MaxNumberOfAttributes))
		i--
		dAtA[i] = 0x20
	}
	if m.Sampler != nil {
		{
			size := m.Sampler.Size()
			i -= size
			if _, err := m.Sampler.MarshalTo(dAtA[i:]); err != nil {
				return 0, err
			}
		}
	}
	return len(dAtA) - i, nil
}

func (m *TraceConfig_ConstantSampler) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *TraceConfig_ConstantSampler) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	if m.ConstantSampler != nil {
		{
			size, err := m.ConstantSampler.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintTraceConfig(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}
func (m *TraceConfig_TraceIdRatioBased) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *TraceConfig_TraceIdRatioBased) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	if m.TraceIdRatioBased != nil {
		{
			size, err := m.TraceIdRatioBased.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintTraceConfig(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x12
	}
	return len(dAtA) - i, nil
}
func (m *TraceConfig_RateLimitingSampler) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *TraceConfig_RateLimitingSampler) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	if m.RateLimitingSampler != nil {
		{
			size, err := m.RateLimitingSampler.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintTraceConfig(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x1a
	}
	return len(dAtA) - i, nil
}
func (m *ConstantSampler) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ConstantSampler) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *ConstantSampler) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Decision != 0 {
		i = encodeVarintTraceConfig(dAtA, i, uint64(m.Decision))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *TraceIdRatioBased) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *TraceIdRatioBased) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *TraceIdRatioBased) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.SamplingRatio != 0 {
		i -= 8
		encoding_binary.LittleEndian.PutUint64(dAtA[i:], uint64(math.Float64bits(float64(m.SamplingRatio))))
		i--
		dAtA[i] = 0x9
	}
	return len(dAtA) - i, nil
}

func (m *RateLimitingSampler) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *RateLimitingSampler) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *RateLimitingSampler) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Qps != 0 {
		i = encodeVarintTraceConfig(dAtA, i, uint64(m.Qps))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func encodeVarintTraceConfig(dAtA []byte, offset int, v uint64) int {
	offset -= sovTraceConfig(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *TraceConfig) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Sampler != nil {
		n += m.Sampler.Size()
	}
	if m.MaxNumberOfAttributes != 0 {
		n += 1 + sovTraceConfig(uint64(m.MaxNumberOfAttributes))
	}
	if m.MaxNumberOfTimedEvents != 0 {
		n += 1 + sovTraceConfig(uint64(m.MaxNumberOfTimedEvents))
	}
	if m.MaxNumberOfAttributesPerTimedEvent != 0 {
		n += 1 + sovTraceConfig(uint64(m.MaxNumberOfAttributesPerTimedEvent))
	}
	if m.MaxNumberOfLinks != 0 {
		n += 1 + sovTraceConfig(uint64(m.MaxNumberOfLinks))
	}
	if m.MaxNumberOfAttributesPerLink != 0 {
		n += 1 + sovTraceConfig(uint64(m.MaxNumberOfAttributesPerLink))
	}
	return n
}

func (m *TraceConfig_ConstantSampler) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.ConstantSampler != nil {
		l = m.ConstantSampler.Size()
		n += 1 + l + sovTraceConfig(uint64(l))
	}
	return n
}
func (m *TraceConfig_TraceIdRatioBased) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.TraceIdRatioBased != nil {
		l = m.TraceIdRatioBased.Size()
		n += 1 + l + sovTraceConfig(uint64(l))
	}
	return n
}
func (m *TraceConfig_RateLimitingSampler) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.RateLimitingSampler != nil {
		l = m.RateLimitingSampler.Size()
		n += 1 + l + sovTraceConfig(uint64(l))
	}
	return n
}
func (m *ConstantSampler) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Decision != 0 {
		n += 1 + sovTraceConfig(uint64(m.Decision))
	}
	return n
}

func (m *TraceIdRatioBased) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.SamplingRatio != 0 {
		n += 9
	}
	return n
}

func (m *RateLimitingSampler) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Qps != 0 {
		n += 1 + sovTraceConfig(uint64(m.Qps))
	}
	return n
}

func sovTraceConfig(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozTraceConfig(x uint64) (n int) {
	return sovTraceConfig(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *TraceConfig) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTraceConfig
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: TraceConfig: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TraceConfig: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field ConstantSampler", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTraceConfig
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthTraceConfig
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTraceConfig
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			v := &ConstantSampler{}
			if err := v.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Sampler = &TraceConfig_ConstantSampler{v}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field TraceIdRatioBased", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTraceConfig
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthTraceConfig
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTraceConfig
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			v := &TraceIdRatioBased{}
			if err := v.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Sampler = &TraceConfig_TraceIdRatioBased{v}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field RateLimitingSampler", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTraceConfig
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthTraceConfig
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTraceConfig
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			v := &RateLimitingSampler{}
			if err := v.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Sampler = &TraceConfig_RateLimitingSampler{v}
			iNdEx = postIndex
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MaxNumberOfAttributes", wireType)
			}
			m.MaxNumberOfAttributes = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTraceConfig
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.MaxNumberOfAttributes |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 5:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MaxNumberOfTimedEvents", wireType)
			}
			m.MaxNumberOfTimedEvents = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTraceConfig
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.MaxNumberOfTimedEvents |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 6:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MaxNumberOfAttributesPerTimedEvent", wireType)
			}
			m.MaxNumberOfAttributesPerTimedEvent = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTraceConfig
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.MaxNumberOfAttributesPerTimedEvent |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 7:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MaxNumberOfLinks", wireType)
			}
			m.MaxNumberOfLinks = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTraceConfig
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.MaxNumberOfLinks |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 8:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MaxNumberOfAttributesPerLink", wireType)
			}
			m.MaxNumberOfAttributesPerLink = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTraceConfig
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.MaxNumberOfAttributesPerLink |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipTraceConfig(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthTraceConfig
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ConstantSampler) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTraceConfig
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: ConstantSampler: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ConstantSampler: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Decision", wireType)
			}
			m.Decision = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTraceConfig
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Decision |= ConstantSampler_ConstantDecision(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipTraceConfig(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthTraceConfig
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *TraceIdRatioBased) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTraceConfig
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: TraceIdRatioBased: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TraceIdRatioBased: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 1 {
				return fmt.Errorf("proto: wrong wireType = %d for field SamplingRatio", wireType)
			}
			var v uint64
			if (iNdEx + 8) > l {
				return io.ErrUnexpectedEOF
			}
			v = uint64(encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			m.SamplingRatio = float64(math.Float64frombits(v))
		default:
			iNdEx = preIndex
			skippy, err := skipTraceConfig(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthTraceConfig
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *RateLimitingSampler) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTraceConfig
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: RateLimitingSampler: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: RateLimitingSampler: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Qps", wireType)
			}
			m.Qps = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTraceConfig
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Qps |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipTraceConfig(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthTraceConfig
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipTraceConfig(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowTraceConfig
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTraceConfig
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTraceConfig
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthTraceConfig
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupTraceConfig
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthTraceConfig
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthTraceConfig        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowTraceConfig          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupTraceConfig = fmt.Errorf("proto: unexpected end of group")
)
