package cbauthx

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"

	"github.com/couchbase/gocbcorex/contrib/jsonrpcx"
	"go.uber.org/zap"
)

type RevRpcClient struct {
	uri      string
	handlers RevRpcHandlers

	conn    *jsonrpcx.Conn
	readBuf jsonrpcx.Request
}

type RevRpcClientOptions struct {
	Logger    *zap.Logger
	TlsConfig *tls.Config
	UserAgent string
	Username  string
	Password  string
}

func NewRevRpcClient(
	ctx context.Context,
	uri string,
	handlers RevRpcHandlers,
	opts *RevRpcClientOptions,
) (*RevRpcClient, error) {
	netConn, err := DialNsRpcConn(ctx, uri, &NsRpcConnOptions{
		TlsConfig: opts.TlsConfig,
		UserAgent: opts.UserAgent,
		Username:  opts.Username,
		Password:  opts.Password,
	})
	if err != nil {
		return nil, &contextualError{
			Message: "failed to dial nsrpc",
			Cause:   err,
		}
	}

	// BUG(MB-55281): We need to check for another response
	// since revrpc always sends a message immediately on connection
	// we can peek some bytes to make sure we aren't receiving a second
	// http response here, allowing us to produce a more useful error
	// rather than a JSON parse error.
	peekErrCh := make(chan error, 1)
	go func() {
		peekBytes, err := netConn.Peek(1)
		if err != nil {
			peekErrCh <- &contextualError{
				Message: "failed to peek response",
				Cause:   err,
			}
			return
		}

		if peekBytes[0] != '{' {
			bodyBytes, readErr := io.ReadAll(netConn)
			if readErr != nil {
				peekErrCh <- &contextualError{
					Message: "failed to read error body for invalid response",
					Cause:   readErr,
				}
				return
			}

			peekErrCh <- &ServerError{
				StatusCode: 200,
				Body:       bodyBytes,
			}
			return
		}

		peekErrCh <- nil
	}()
	select {
	case err := <-peekErrCh:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		_ = netConn.Close()
		<-peekErrCh
		return nil, &contextualError{
			Message: "context cancelled while peeking response",
			Cause:   ctx.Err(),
		}
	}

	client := &RevRpcClient{
		uri:      uri,
		handlers: handlers,
		conn:     jsonrpcx.NewConn(netConn),
	}

	return client, nil
}

func (r *RevRpcClient) Run() error {
	req := &r.readBuf

	for {
		// clear the cached request, but keep our buffers
		*req = jsonrpcx.Request{
			Params: req.Params[:0],
		}

		// Read the request
		err := r.conn.ReadRequest(req)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				// if the connection was explicitly closed, we can just return
				// a no-error since the user initiated the close.
				return nil
			}

			return &contextualError{
				Message: "failed to read request",
				Cause:   err,
			}
		}

		var resp *jsonrpcx.Response
		switch req.Method {
		case "AuthCacheSvc.UpdateDB":
			resp = jsonrpcx.Proc1ArgMethod(req, r.handlers.UpdateDB)
		case "AuthCacheSvc.UpdateDBExt":
			resp = jsonrpcx.Proc1ArgMethod(req, r.handlers.UpdateDBExt)
		case "AuthCacheSvc.Heartbeat":
			resp = jsonrpcx.Proc1ArgMethod(req, r.handlers.Heartbeat)
		default:
			resp = jsonrpcx.ProcUnknownMethod(req, func(method string, params interface{}) {
				log.Printf("received unexpected request (method:%s, params:%+v)", method, params)
			})
		}

		err = r.conn.WriteResponse(resp)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				// if the connection was explicitly closed, we can just return
				// a no-error since the user initiated the close.
				return nil
			}

			return &contextualError{
				Message: "failed to write response",
				Cause:   err,
			}
		}
	}
}

func (r *RevRpcClient) Close() error {
	return r.conn.Close()
}
