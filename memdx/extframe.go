package memdx

func AppendExtFrame(frameCode ExtFrameCode, frameBody []byte, buf []byte) ([]byte, error) {
	frameLen := len(frameBody)

	// add the header
	buf = append(buf, 0)

	if frameCode < 15 {
		buf[0] = buf[0] | (byte(frameCode&0xF) << 4)
	} else {
		if frameCode-15 >= 15 {
			return nil, protocolError{"extframe code too large to encode"}
		}

		buf[0] = buf[0] | 0xF0
		buf = append(buf, byte(frameCode-15))
	}

	if frameLen < 15 {
		buf[0] = buf[0] | (byte(frameLen&0xF) << 0)
	} else {
		if frameLen-15 >= 15 {
			return nil, protocolError{"extframe len too large to encode"}
		}

		buf[0] = buf[0] | 0x0F
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
