package cbauthx

import "context"

type UserInfo struct {
	Domain string
	Uuid   string
}

type AuthCheck interface {
	CheckUserPass(ctx context.Context, username string, password string) (UserInfo, error)
}
