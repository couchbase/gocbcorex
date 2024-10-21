package asyncwritebuf

import (
	"bufio"
	"io"
	"sync"
	"sync/atomic"
)

type Writer struct {
	writer *bufio.Writer

	lock         sync.Mutex
	closed       bool
	bytesSig     *sync.Cond
	roomSig      *sync.Cond
	err          error
	bufs         [][]byte
	pendingBytes atomic.Int64
}

var _ io.Writer = (*Writer)(nil)

func NewWriter(w io.Writer, size int) *Writer {
	b := &Writer{
		writer: bufio.NewWriterSize(w, size),
		bufs:   make([][]byte, 0, 1024),
	}

	b.bytesSig = sync.NewCond(&b.lock)
	b.roomSig = sync.NewCond(&b.lock)

	go b.run()

	return b
}

func (w *Writer) run() {
	bufs := make([][]byte, 0, 1024)

	w.lock.Lock()
	for {
		if w.closed && len(w.bufs) == 0 {
			break
		}

		if w.err != nil {
			break
		}

		if len(w.bufs) == 0 {
			w.bytesSig.Wait()
			continue
		}

		bufs = bufs[:0]
		bufs = append(bufs, w.bufs...)
		w.bufs = w.bufs[:0]

		w.lock.Unlock()

		w.roomSig.Broadcast()

		var err error
		var writtenBytes int
		for _, buf := range bufs {
			bufLen := len(buf)
			writtenBytes += bufLen

			_, err = w.writer.Write(buf)
			if err != nil {
				break
			}
		}

		newPendingBytes := w.pendingBytes.Add(int64(-writtenBytes))

		if err == nil {
			// this is less or equal to handle a race between the buffer being added,
			// and the pending bytes being updated inside the lock below.
			if newPendingBytes <= 0 {
				err = w.writer.Flush()
			}
		}

		w.lock.Lock()

		if err != nil {
			w.err = err
			w.roomSig.Broadcast()
			break
		}
	}
	w.lock.Unlock()
}

func (w *Writer) Write(p []byte) (int, error) {
	writeBufferSize := w.writer.Size()

	w.lock.Lock()

	bufLen := len(p)

	if w.closed {
		w.lock.Unlock()
		return 0, io.EOF
	}

	err := w.err
	if err != nil {
		w.lock.Unlock()
		return 0, err
	}

	for {
		err := w.err
		if err != nil {
			w.lock.Unlock()
			return 0, err
		}

		pendingBytes := w.pendingBytes.Load()
		if pendingBytes < int64(writeBufferSize) {
			break
		}

		w.roomSig.Wait()
	}

	w.bufs = append(w.bufs, p)
	w.pendingBytes.Add(int64(bufLen))

	w.lock.Unlock()

	w.bytesSig.Signal()

	return bufLen, nil
}

func (w *Writer) Close() error {
	w.lock.Lock()

	w.closed = true

	w.lock.Unlock()

	w.bytesSig.Signal()
	w.roomSig.Broadcast()

	return nil
}
