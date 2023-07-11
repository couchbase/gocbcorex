package memdx

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// Parses the time embedded in a CAS
func ParseCasToTime(in uint64) (time.Time, error) {
	// A CAS is a HLC where the current unix time in nanoseconds is taken
	// and the lower 16 bits are removed, then they are replaced with the
	// hybrid part of the clock to produce the final CAS value.

	// mask off the bottom 16 bits
	masked := in & ^uint64((1<<16)-1)

	// return this value as a unix nanosecond epoch
	return time.Unix(0, int64(masked)), nil
}

// Parses an HLC returned by an xattr macro fetch into a timestamp.
func ParseHLCToTime(in []byte) (time.Time, error) {
	jsonHLC := struct {
		NowSecsStr string `json:"now"`
	}{}

	err := json.Unmarshal(in, &jsonHLC)
	if err != nil {
		return time.Time{}, err
	}

	unixSecs, err := strconv.ParseInt(jsonHLC.NowSecsStr, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(unixSecs, 0), nil
}

func ParseMacroCasToCas(in []byte) (uint64, error) {
	if len(in) != 18 {
		return 0, fmt.Errorf("provided cas value is the wrong length: %s", in)
	}

	if in[0] != '0' || in[1] != 'x' {
		return 0, fmt.Errorf("provided cas value is the wrong format: %s", in)
	}

	hexIn := in[2:]

	hexVal, err := strconv.ParseUint(string(hexIn), 16, 64)
	if err != nil {
		return 0, nil
	}

	// Due to a bug in the server, the CAS value is encoded backwards...
	// From Java impl:
	// ${Mutation.CAS} is written by kvengine with 'macroToString(htonll(info.cas))'.  Discussed this with KV team and,
	// though there is consensus that this is off (htonll is definitely wrong, and a string is an odd choice), there are
	// clients (SyncGateway) that consume the current string, so it can't be changed.  Note that only little-endian
	// servers are supported for Couchbase, so the 8 byte long inside the string will always be little-endian ordered.
	return ((hexVal>>0)&0xFF)<<56 |
		((hexVal>>8)&0xFF)<<48 |
		((hexVal>>16)&0xFF)<<40 |
		((hexVal>>24)&0xFF)<<32 |
		((hexVal>>32)&0xFF)<<24 |
		((hexVal>>40)&0xFF)<<16 |
		((hexVal>>48)&0xFF)<<8 |
		((hexVal>>56)&0xFF)<<0, nil
}
