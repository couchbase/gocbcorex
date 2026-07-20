package gocbcorex

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestPasswordAuthenticatorLoggingDoesNotLeakPassword(t *testing.T) {
	auth := &PasswordAuthenticator{
		Username: "Administrator",
		Password: "s3cr3tpassw0rd",
	}

	encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())

	fields := []zap.Field{
		// reflection-based logging of a struct containing the authenticator,
		// mirroring how configs were inadvertently logged
		zap.Any("config", struct {
			Authenticator Authenticator
		}{
			Authenticator: auth,
		}),
		zap.Any("auth", auth),
		zap.Object("authObj", auth),
	}

	buf, err := encoder.EncodeEntry(zapcore.Entry{Message: "test"}, fields)
	require.NoError(t, err)

	assert.NotContains(t, buf.String(), auth.Password)
	assert.Contains(t, buf.String(), auth.Username)
}
