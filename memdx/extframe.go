package memdx

import (
	"math"
	"time"
)

func AppendExtFrame(frameCode ExtFrameCode, frameBody []byte, buf []byte) ([]byte, error) {
	frameLen := len(frameBody)

	// add the header
	buf = append(buf, 0)
	hdrBytePtr := &buf[len(buf)-1]

	if frameCode < 15 {
		*hdrBytePtr = *hdrBytePtr | (byte(frameCode&0xF) << 4)
	} else {
		if frameCode-15 >= 15 {
			return nil, protocolError{"extframe code too large to encode"}
		}

		*hdrBytePtr = *hdrBytePtr | 0xF0
		buf = append(buf, byte(frameCode-15))
	}

	if frameLen < 15 {
		*hdrBytePtr = *hdrBytePtr | (byte(frameLen&0xF) << 0)
	} else {
		if frameLen-15 >= 15 {
			return nil, protocolError{"extframe len too large to encode"}
		}

		*hdrBytePtr = *hdrBytePtr | 0x0F
		buf = append(buf, byte(frameLen-15))
	}

	if len(frameBody) > 0 {
		buf = append(buf, frameBody...)
	}

	return buf, nil
}

func DecodeExtFrame(buf []byte) (ExtFrameCode, []byte, int, error) {
	if len(buf) < 1 {
		return 0, nil, 0, protocolError{"framing extras protocol error"}
	}

	bufPos := 0

	frameHeader := buf[bufPos]
	frameCode := ExtFrameCode((frameHeader & 0xF0) >> 4)
	frameLen := uint((frameHeader & 0x0F) >> 0)
	bufPos++

	if frameCode == 15 {
		if len(buf) < bufPos+1 {
			return 0, nil, 0, protocolError{"unexpected eof"}
		}

		frameCodeExt := buf[bufPos]
		frameCode = ExtFrameCode(15 + frameCodeExt)
		bufPos++
	}

	if frameLen == 15 {
		if len(buf) < bufPos+1 {
			return 0, nil, 0, protocolError{"unexpected eof"}
		}

		frameLenExt := buf[bufPos]
		frameLen = uint(15 + frameLenExt)
		bufPos++
	}

	intFrameLen := int(frameLen)
	if len(buf) < bufPos+intFrameLen {
		return 0, nil, 0, protocolError{"unexpected eof"}
	}

	frameBody := buf[bufPos : bufPos+intFrameLen]
	bufPos += intFrameLen

	return frameCode, frameBody, bufPos, nil
}

func IterExtFrames(buf []byte, cb func(ExtFrameCode, []byte)) error {
	for len(buf) > 0 {
		frameCode, frameBody, n, err := DecodeExtFrame(buf)
		if err != nil {
			return err
		}

		cb(frameCode, frameBody)

		buf = buf[n:]
	}

	return nil
}

func EncodeDurabilityExtFrame(
	level DurabilityLevel,
	timeout time.Duration,
) ([]byte, error) {
	if level == 0 {
		return nil, &protocolError{"cannot encode durability without a level"}
	}

	if timeout == 0 {
		return []byte{byte(level)}, nil
	}

	timeoutMillis := uint64(timeout / time.Millisecond)
	if timeoutMillis > 65535 {
		return nil, &protocolError{"cannot encode durability timeout greater than 65535 milliseconds"}
	} else if timeoutMillis == 0 {
		timeoutMillis = 1
	}

	return []byte{
		byte(level),
		uint8(timeoutMillis >> 8),
		byte(timeoutMillis),
	}, nil
}

func DecodeDurabilityExtFrame(
	buf []byte,
) (DurabilityLevel, time.Duration, error) {
	if len(buf) == 1 {
		durabilityLevel := DurabilityLevel(buf[0])
		return durabilityLevel, 0, nil
	} else if len(buf) == 3 {
		durabilityLevel := DurabilityLevel(buf[0])
		timeoutMillis := uint64(buf[1])<<8 | uint64(buf[2])
		timeout := time.Duration(timeoutMillis) * time.Millisecond
		return durabilityLevel, timeout, nil
	}

	return 0, 0, &protocolError{"invalid durability extframe length"}
}

func EncodeServerDurationExtFrame(
	dura time.Duration,
) ([]byte, error) {
	duraUs := dura / time.Microsecond
	duraEnc := int(math.Pow(float64(duraUs)*2, 1.0/1.74))
	if duraEnc > 65535 {
		duraEnc = 65535
	}

	return []byte{
		byte(duraEnc >> 8),
		byte(duraEnc),
	}, nil
}

func DecodeServerDurationExtFrame(
	buf []byte,
) (time.Duration, error) {
	if len(buf) != 2 {
		return 0, &protocolError{"invalid server duration extframe length"}
	}

	duraEnc := uint64(buf[0])<<8 | uint64(buf[1])
	duraUs := math.Round(math.Pow(float64(duraEnc), 1.74) / 2)
	dura := time.Duration(duraUs) * time.Microsecond

	return dura, nil
}
