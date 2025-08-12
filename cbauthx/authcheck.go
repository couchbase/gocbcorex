package cbauthx

import "context"

type AuthCheck interface {
	CheckUserPass(ctx context.Context, username string, password string) (UserInfo, error)
}
