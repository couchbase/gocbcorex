package gocbcorex

/*
type DcpStream struct {
	parent   *DcpStreamGroup
	handlers DcpEventsHandlers
	cli      *DcpStreamClient
	vbId     uint16
}

func (s *DcpStream) Close(ctx context.Context) error {
	_, err := s.cli.client.DcpCloseStream(ctx, &memdx.DcpCloseStreamRequest{
		VbucketID: s.vbId,
	})
	if err != nil {
		return err
	}

	if !s.cli.client.StreamEndOnCloseEnabled() {
		// if stream-end-on-close is not enabled, we need to manually send the event
		// to tell users that the stream has ended...  the server provides a guarentee
		// in this case that no further events will be emitted after the close-stream
		// response is sent.
		s.handlers.DcpStreamEnd(&DcpStreamEndEvent{
			VbucketId: s.vbId,
		})
	}

	return nil
}
*/
