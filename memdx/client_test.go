package memdx

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// When the read loop exits it must close the connection itself, so that
// read error handlers don't need to reach back into the client to do it.
func TestClientClosesConnWhenReadLoopExits(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	go func() {
		serverConn, err := listener.Accept()
		if err != nil {
			return
		}
		_ = serverConn.Close()
	}()

	conn, err := DialConn(context.Background(), listener.Addr().String(), nil)
	require.NoError(t, err)

	readErrCh := make(chan error, 1)
	cli := NewClient(conn, &ClientOptions{
		ReadErrorHandler: func(err error) {
			// The connection must already be closed when the handler runs.
			readErrCh <- conn.WritePacket(&Packet{OpCode: OpCodeNoOp})
		},
	})

	select {
	case writeErr := <-readErrCh:
		require.ErrorIs(t, writeErr, net.ErrClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("read error handler was not called")
	}

	require.ErrorIs(t, cli.Close(), net.ErrClosed)
}
