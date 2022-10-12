// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.5
// source: cluster.proto

package api

import (
	rtapi "github.com/heroiclabs/nakama-common/rtapi"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// 接口
type Envelope struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id   uint64 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	Node string `protobuf:"bytes,2,opt,name=node,proto3" json:"node,omitempty"`
	// Types that are assignable to Payload:
	//	*Envelope_NakamaRtapi
	//	*Envelope_Bytes
	//	*Envelope_Error
	Payload isEnvelope_Payload `protobuf_oneof:"payload"`
}

func (x *Envelope) Reset() {
	*x = Envelope{}
	if protoimpl.UnsafeEnabled {
		mi := &file_cluster_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Envelope) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Envelope) ProtoMessage() {}

func (x *Envelope) ProtoReflect() protoreflect.Message {
	mi := &file_cluster_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Envelope.ProtoReflect.Descriptor instead.
func (*Envelope) Descriptor() ([]byte, []int) {
	return file_cluster_proto_rawDescGZIP(), []int{0}
}

func (x *Envelope) GetId() uint64 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *Envelope) GetNode() string {
	if x != nil {
		return x.Node
	}
	return ""
}

func (m *Envelope) GetPayload() isEnvelope_Payload {
	if m != nil {
		return m.Payload
	}
	return nil
}

func (x *Envelope) GetNakamaRtapi() *rtapi.Envelope {
	if x, ok := x.GetPayload().(*Envelope_NakamaRtapi); ok {
		return x.NakamaRtapi
	}
	return nil
}

func (x *Envelope) GetBytes() *Bytes {
	if x, ok := x.GetPayload().(*Envelope_Bytes); ok {
		return x.Bytes
	}
	return nil
}

func (x *Envelope) GetError() *Error {
	if x, ok := x.GetPayload().(*Envelope_Error); ok {
		return x.Error
	}
	return nil
}

type isEnvelope_Payload interface {
	isEnvelope_Payload()
}

type Envelope_NakamaRtapi struct {
	NakamaRtapi *rtapi.Envelope `protobuf:"bytes,3,opt,name=NakamaRtapi,proto3,oneof"`
}

type Envelope_Bytes struct {
	Bytes *Bytes `protobuf:"bytes,4,opt,name=bytes,proto3,oneof"`
}

type Envelope_Error struct {
	Error *Error `protobuf:"bytes,5,opt,name=error,proto3,oneof"`
}

func (*Envelope_NakamaRtapi) isEnvelope_Payload() {}

func (*Envelope_Bytes) isEnvelope_Payload() {}

func (*Envelope_Error) isEnvelope_Payload() {}

type NodeStatus struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Node   string `protobuf:"bytes,1,opt,name=node,proto3" json:"node,omitempty"`
	Status int32  `protobuf:"varint,2,opt,name=status,proto3" json:"status,omitempty"`
	Join   bool   `protobuf:"varint,3,opt,name=join,proto3" json:"join,omitempty"`
}

func (x *NodeStatus) Reset() {
	*x = NodeStatus{}
	if protoimpl.UnsafeEnabled {
		mi := &file_cluster_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NodeStatus) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NodeStatus) ProtoMessage() {}

func (x *NodeStatus) ProtoReflect() protoreflect.Message {
	mi := &file_cluster_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NodeStatus.ProtoReflect.Descriptor instead.
func (*NodeStatus) Descriptor() ([]byte, []int) {
	return file_cluster_proto_rawDescGZIP(), []int{1}
}

func (x *NodeStatus) GetNode() string {
	if x != nil {
		return x.Node
	}
	return ""
}

func (x *NodeStatus) GetStatus() int32 {
	if x != nil {
		return x.Status
	}
	return 0
}

func (x *NodeStatus) GetJoin() bool {
	if x != nil {
		return x.Join
	}
	return false
}

// bytes
type Bytes struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Content []byte `protobuf:"bytes,1,opt,name=Content,proto3" json:"Content,omitempty"`
}

func (x *Bytes) Reset() {
	*x = Bytes{}
	if protoimpl.UnsafeEnabled {
		mi := &file_cluster_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Bytes) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Bytes) ProtoMessage() {}

func (x *Bytes) ProtoReflect() protoreflect.Message {
	mi := &file_cluster_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Bytes.ProtoReflect.Descriptor instead.
func (*Bytes) Descriptor() ([]byte, []int) {
	return file_cluster_proto_rawDescGZIP(), []int{2}
}

func (x *Bytes) GetContent() []byte {
	if x != nil {
		return x.Content
	}
	return nil
}

// error
type Error struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// The error code which should be one of "Error.Code" enums.
	Code int32 `protobuf:"varint,1,opt,name=code,proto3" json:"code,omitempty"`
	// A message in English to help developers debug the response.
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	// Additional error details which may be different for each response.
	Context map[string]string `protobuf:"bytes,3,rep,name=context,proto3" json:"context,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *Error) Reset() {
	*x = Error{}
	if protoimpl.UnsafeEnabled {
		mi := &file_cluster_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Error) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Error) ProtoMessage() {}

func (x *Error) ProtoReflect() protoreflect.Message {
	mi := &file_cluster_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Error.ProtoReflect.Descriptor instead.
func (*Error) Descriptor() ([]byte, []int) {
	return file_cluster_proto_rawDescGZIP(), []int{3}
}

func (x *Error) GetCode() int32 {
	if x != nil {
		return x.Code
	}
	return 0
}

func (x *Error) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

func (x *Error) GetContext() map[string]string {
	if x != nil {
		return x.Context
	}
	return nil
}

var File_cluster_proto protoreflect.FileDescriptor

var file_cluster_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x0e, 0x6e, 0x61, 0x6b, 0x61, 0x6d, 0x61, 0x2e, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x1a,
	0x14, 0x72, 0x74, 0x61, 0x70, 0x69, 0x2f, 0x72, 0x65, 0x61, 0x6c, 0x74, 0x69, 0x6d, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xd6, 0x01, 0x0a, 0x08, 0x45, 0x6e, 0x76, 0x65, 0x6c, 0x6f,
	0x70, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x02,
	0x69, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x6f, 0x64, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x04, 0x6e, 0x6f, 0x64, 0x65, 0x12, 0x3d, 0x0a, 0x0b, 0x4e, 0x61, 0x6b, 0x61, 0x6d, 0x61,
	0x52, 0x74, 0x61, 0x70, 0x69, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x6e, 0x61,
	0x6b, 0x61, 0x6d, 0x61, 0x2e, 0x72, 0x65, 0x61, 0x6c, 0x74, 0x69, 0x6d, 0x65, 0x2e, 0x45, 0x6e,
	0x76, 0x65, 0x6c, 0x6f, 0x70, 0x65, 0x48, 0x00, 0x52, 0x0b, 0x4e, 0x61, 0x6b, 0x61, 0x6d, 0x61,
	0x52, 0x74, 0x61, 0x70, 0x69, 0x12, 0x2d, 0x0a, 0x05, 0x62, 0x79, 0x74, 0x65, 0x73, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x6e, 0x61, 0x6b, 0x61, 0x6d, 0x61, 0x2e, 0x63, 0x6c,
	0x75, 0x73, 0x74, 0x65, 0x72, 0x2e, 0x42, 0x79, 0x74, 0x65, 0x73, 0x48, 0x00, 0x52, 0x05, 0x62,
	0x79, 0x74, 0x65, 0x73, 0x12, 0x2d, 0x0a, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x6e, 0x61, 0x6b, 0x61, 0x6d, 0x61, 0x2e, 0x63, 0x6c, 0x75,
	0x73, 0x74, 0x65, 0x72, 0x2e, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x48, 0x00, 0x52, 0x05, 0x65, 0x72,
	0x72, 0x6f, 0x72, 0x42, 0x09, 0x0a, 0x07, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x22, 0x4c,
	0x0a, 0x0a, 0x4e, 0x6f, 0x64, 0x65, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x12, 0x0a, 0x04,
	0x6e, 0x6f, 0x64, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x6f, 0x64, 0x65,
	0x12, 0x16, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05,
	0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x12, 0x0a, 0x04, 0x6a, 0x6f, 0x69, 0x6e,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x08, 0x52, 0x04, 0x6a, 0x6f, 0x69, 0x6e, 0x22, 0x21, 0x0a, 0x05,
	0x42, 0x79, 0x74, 0x65, 0x73, 0x12, 0x18, 0x0a, 0x07, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x22,
	0xaf, 0x01, 0x0a, 0x05, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x12, 0x12, 0x0a, 0x04, 0x63, 0x6f, 0x64,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x63, 0x6f, 0x64, 0x65, 0x12, 0x18, 0x0a,
	0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07,
	0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x3c, 0x0a, 0x07, 0x63, 0x6f, 0x6e, 0x74, 0x65,
	0x78, 0x74, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x22, 0x2e, 0x6e, 0x61, 0x6b, 0x61, 0x6d,
	0x61, 0x2e, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x2e, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x2e,
	0x43, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x07, 0x63, 0x6f,
	0x6e, 0x74, 0x65, 0x78, 0x74, 0x1a, 0x3a, 0x0a, 0x0c, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74,
	0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38,
	0x01, 0x32, 0x8d, 0x01, 0x0a, 0x09, 0x41, 0x70, 0x69, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x12,
	0x3c, 0x0a, 0x04, 0x43, 0x61, 0x6c, 0x6c, 0x12, 0x18, 0x2e, 0x6e, 0x61, 0x6b, 0x61, 0x6d, 0x61,
	0x2e, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x2e, 0x45, 0x6e, 0x76, 0x65, 0x6c, 0x6f, 0x70,
	0x65, 0x1a, 0x18, 0x2e, 0x6e, 0x61, 0x6b, 0x61, 0x6d, 0x61, 0x2e, 0x63, 0x6c, 0x75, 0x73, 0x74,
	0x65, 0x72, 0x2e, 0x45, 0x6e, 0x76, 0x65, 0x6c, 0x6f, 0x70, 0x65, 0x22, 0x00, 0x12, 0x42, 0x0a,
	0x06, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x12, 0x18, 0x2e, 0x6e, 0x61, 0x6b, 0x61, 0x6d, 0x61,
	0x2e, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x2e, 0x45, 0x6e, 0x76, 0x65, 0x6c, 0x6f, 0x70,
	0x65, 0x1a, 0x18, 0x2e, 0x6e, 0x61, 0x6b, 0x61, 0x6d, 0x61, 0x2e, 0x63, 0x6c, 0x75, 0x73, 0x74,
	0x65, 0x72, 0x2e, 0x45, 0x6e, 0x76, 0x65, 0x6c, 0x6f, 0x70, 0x65, 0x22, 0x00, 0x28, 0x01, 0x30,
	0x01, 0x42, 0x28, 0x5a, 0x26, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x64, 0x6f, 0x75, 0x62, 0x6c, 0x65, 0x6d, 0x6f, 0x2f, 0x6e, 0x61, 0x6b, 0x61, 0x6d, 0x61, 0x2d,
	0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x2f, 0x61, 0x70, 0x69, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
}

var (
	file_cluster_proto_rawDescOnce sync.Once
	file_cluster_proto_rawDescData = file_cluster_proto_rawDesc
)

func file_cluster_proto_rawDescGZIP() []byte {
	file_cluster_proto_rawDescOnce.Do(func() {
		file_cluster_proto_rawDescData = protoimpl.X.CompressGZIP(file_cluster_proto_rawDescData)
	})
	return file_cluster_proto_rawDescData
}

var file_cluster_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_cluster_proto_goTypes = []interface{}{
	(*Envelope)(nil),       // 0: nakama.cluster.Envelope
	(*NodeStatus)(nil),     // 1: nakama.cluster.NodeStatus
	(*Bytes)(nil),          // 2: nakama.cluster.Bytes
	(*Error)(nil),          // 3: nakama.cluster.Error
	nil,                    // 4: nakama.cluster.Error.ContextEntry
	(*rtapi.Envelope)(nil), // 5: nakama.realtime.Envelope
}
var file_cluster_proto_depIdxs = []int32{
	5, // 0: nakama.cluster.Envelope.NakamaRtapi:type_name -> nakama.realtime.Envelope
	2, // 1: nakama.cluster.Envelope.bytes:type_name -> nakama.cluster.Bytes
	3, // 2: nakama.cluster.Envelope.error:type_name -> nakama.cluster.Error
	4, // 3: nakama.cluster.Error.context:type_name -> nakama.cluster.Error.ContextEntry
	0, // 4: nakama.cluster.ApiServer.Call:input_type -> nakama.cluster.Envelope
	0, // 5: nakama.cluster.ApiServer.Stream:input_type -> nakama.cluster.Envelope
	0, // 6: nakama.cluster.ApiServer.Call:output_type -> nakama.cluster.Envelope
	0, // 7: nakama.cluster.ApiServer.Stream:output_type -> nakama.cluster.Envelope
	6, // [6:8] is the sub-list for method output_type
	4, // [4:6] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_cluster_proto_init() }
func file_cluster_proto_init() {
	if File_cluster_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_cluster_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Envelope); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_cluster_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NodeStatus); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_cluster_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Bytes); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_cluster_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Error); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_cluster_proto_msgTypes[0].OneofWrappers = []interface{}{
		(*Envelope_NakamaRtapi)(nil),
		(*Envelope_Bytes)(nil),
		(*Envelope_Error)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_cluster_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_cluster_proto_goTypes,
		DependencyIndexes: file_cluster_proto_depIdxs,
		MessageInfos:      file_cluster_proto_msgTypes,
	}.Build()
	File_cluster_proto = out.File
	file_cluster_proto_rawDesc = nil
	file_cluster_proto_goTypes = nil
	file_cluster_proto_depIdxs = nil
}
