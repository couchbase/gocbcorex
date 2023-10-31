package cbqueryx

import (
	"encoding/json"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
)

type ScanConsistency string

const (
	ScanConsistencyUnset       ScanConsistency = ""
	ScanConsistencyNotBounded  ScanConsistency = "not_bounded"
	ScanConsistencyRequestPlus ScanConsistency = "request_plus"
)

type ProfileMode string

const (
	ProfileModeUnset   ProfileMode = ""
	ProfileModeOff     ProfileMode = "off"
	ProfileModePhases  ProfileMode = "phases"
	ProfileModeTimings ProfileMode = "timings"
)

type Compression string

const (
	CompressionUnset Compression = ""
	CompressionZip   Compression = "ZIP"
	CompressionRle   Compression = "RLE"
	CompressionLzma  Compression = "LZMA"
	CompressionLzo   Compression = "LZO"
	CompressionNone  Compression = "NONE"
)

type DurabilityLevel string

const (
	DurabilityLevelUnset                    DurabilityLevel = ""
	DurabilityLevelNone                     DurabilityLevel = "none"
	DurabilityLevelMajority                 DurabilityLevel = "majority"
	DurabilityLevelMajorityAndPersistActive DurabilityLevel = "majorityAndPersistActive"
	DurabilityLevelPersistToMajority        DurabilityLevel = "persistToMajority"
)

type Encoding string

const (
	EncodingUnset Encoding = ""
	EncodingUtf8  Encoding = "UTF-8"
)

type Format string

const (
	FormatUnset Format = ""
	FormatJson  Format = "JSON"
	FormatXml   Format = "XML"
	FormatCsv   Format = "CSV"
	FormatTsv   Format = "TSV"
)

type CredsJson struct {
	User string `json:"user,omitempty"`
	Pass string `json:"pass,omitempty"`
}

type ScanVectorEntry struct {
	SeqNo  uint64
	VbUuid string
}

func (e ScanVectorEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal([]interface{}{e.SeqNo, e.VbUuid})
}

func (e ScanVectorEntry) UnmarshalJSON(data []byte) error {
	return nil
}

var _ json.Marshaler = (*ScanVectorEntry)(nil)
var _ json.Unmarshaler = (*ScanVectorEntry)(nil)

type FullScanVectors []ScanVectorEntry

type SparseScanVectors map[uint32]ScanVectorEntry

type Options struct {
	Args            []json.RawMessage
	AtrCollection   string
	AutoExecute     bool
	ClientContextId string
	Compression     Compression
	Controls        bool
	Creds           []CredsJson
	DurabilityLevel DurabilityLevel
	EncodedPlan     string
	Encoding        Encoding
	Format          Format
	KvTimeout       time.Duration
	MaxParallelism  uint32
	MemoryQuota     uint32
	Metrics         bool
	Namespace       string
	NumAtrs         uint32
	PipelineBatch   uint32
	PipelineCap     uint32
	Prepared        string
	PreserveExpiry  bool
	Pretty          bool
	Profile         ProfileMode
	QueryContext    string
	ReadOnly        bool
	ScanCap         uint32
	ScanConsistency ScanConsistency
	ScanVector      json.RawMessage
	ScanVectors     map[string]json.RawMessage
	ScanWait        time.Duration
	Signature       bool
	Statement       string
	Timeout         time.Duration
	TxData          json.RawMessage
	TxId            string
	TxImplicit      bool
	TxStmtNum       uint32
	TxTimeout       time.Duration
	UseCbo          bool
	UseFts          bool

	NamedArgs map[string]json.RawMessage
	Raw       map[string]json.RawMessage

	OnBehalfOf *cbhttpx.OnBehalfOfInfo
}

func (o *Options) encodeToJson() (json.RawMessage, error) {
	var anyErr error

	m := make(map[string]json.RawMessage)

	encodeField := func(val interface{}) json.RawMessage {
		// if any previous error occured, just skip this encoding
		if anyErr != nil {
			return nil
		}

		// attempt to encode the field
		bytes, err := json.Marshal(val)
		if err != nil {
			anyErr = err
			return nil
		}

		return bytes
	}

	if len(o.Args) > 0 {
		m["args"] = encodeField(o.Args)
	}
	if o.AtrCollection != "" {
		m["atr_collection"] = encodeField(o.AtrCollection)
	}
	if o.AutoExecute {
		m["auto_execute"] = encodeField(true)
	}
	if o.ClientContextId != "" {
		m["client_context_id"] = encodeField(o.ClientContextId)
	}
	if o.Compression != CompressionUnset {
		m["compression"] = encodeField(o.Compression)
	}
	if o.Controls {
		m["controls"] = encodeField(true)
	}
	if len(o.Creds) > 0 {
		m["creds"] = encodeField(o.Creds)
	}
	if o.DurabilityLevel != DurabilityLevelUnset {
		m["durability_level"] = encodeField(o.DurabilityLevel)
	}
	if o.EncodedPlan != "" {
		m["encoded_plan"] = encodeField(o.EncodedPlan)
	}
	if o.Encoding != EncodingUnset {
		m["encoding"] = encodeField(o.Encoding)
	}
	if o.Format != FormatUnset {
		m["format"] = encodeField(o.Format)
	}
	if o.KvTimeout > 0 {
		m["kv_timeout"] = encodeField(o.KvTimeout)
	}
	if o.MaxParallelism > 0 {
		m["max_parallelism"] = encodeField(o.MaxParallelism)
	}
	if o.MemoryQuota > 0 {
		m["memory_quota"] = encodeField(o.MemoryQuota)
	}
	if o.Metrics {
		m["metrics"] = encodeField(true)
	}
	if o.Namespace != "" {
		m["namespace"] = encodeField(o.Namespace)
	}
	if o.NumAtrs > 0 {
		m["num_atrs"] = encodeField(o.NumAtrs)
	}
	if o.PipelineBatch > 0 {
		m["pipeline_batch"] = encodeField(o.PipelineBatch)
	}
	if o.PipelineCap > 0 {
		m["pipeline_cap"] = encodeField(o.PipelineCap)
	}
	if o.Prepared != "" {
		m["prepared"] = encodeField(o.Prepared)
	}
	if o.PreserveExpiry {
		m["preserve_expiry"] = encodeField(true)
	}
	if o.Pretty {
		m["pretty"] = encodeField(true)
	}
	if o.Profile != ProfileModeUnset {
		m["profile"] = encodeField(o.Profile)
	}
	if o.QueryContext != "" {
		m["query_context"] = encodeField(o.QueryContext)
	}
	if o.ReadOnly {
		m["readonly"] = encodeField(true)
	}
	if o.ScanCap > 0 {
		m["scan_cap"] = encodeField(o.ScanCap)
	}
	if o.ScanConsistency != ScanConsistencyUnset {
		m["scan_consistency"] = encodeField(o.ScanConsistency)
	}
	if len(o.ScanVector) > 0 {
		m["scan_vector"] = encodeField(o.ScanVector)
	}
	if o.ScanVectors != nil {
		m["scan_vectors"] = encodeField(o.ScanVectors)
	}
	if o.ScanWait > 0 {
		m["scan_wait"] = encodeField(o.ScanWait)
	}
	if o.Signature {
		m["signature"] = encodeField(true)
	}
	if o.Statement != "" {
		m["statement"] = encodeField(o.Statement)
	}
	if o.Timeout > 0 {
		m["timeout"] = encodeField(o.Timeout)
	}
	if len(o.TxData) > 0 {
		m["txdata"] = encodeField(o.TxData)
	}
	if o.TxId != "" {
		m["txid"] = encodeField(o.TxId)
	}
	if o.TxImplicit {
		m["tximplicit"] = encodeField(true)
	}
	if o.TxStmtNum > 0 {
		m["txstmtnum"] = encodeField(o.TxStmtNum)
	}
	if o.TxTimeout > 0 {
		m["txtimeout"] = encodeField(o.TxTimeout)
	}
	if o.UseCbo {
		m["use_cbo"] = encodeField(true)
	}
	if o.UseFts {
		m["use_fts"] = encodeField(true)
	}

	for k, v := range o.NamedArgs {
		m["$"+k] = v
	}

	for k, v := range o.Raw {
		m[k] = v
	}

	if anyErr != nil {
		return nil, anyErr
	}

	return json.Marshal(m)
}
