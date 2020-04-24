package db

import (
	"context"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	ycsdk "github.com/yandex-cloud/go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk"
)

var _ ydb.Credentials = &cachedSDKCredentials{}

type cachedSDKCredentials struct {
	mux           *sync.RWMutex
	token         string
	expiresAt     time.Time
	renewOverhead time.Duration
	sdk           *ycsdk.SDK
}

func (c *cachedSDKCredentials) Token(ctx context.Context) (string, error) {
	c.mux.RLock()
	if c.expiresAt.Add(c.renewOverhead).Before(time.Now()) {
		defer c.mux.RUnlock()
		return c.token, nil
	}

	//renew
	c.mux.RUnlock()
	c.mux.Lock()
	defer c.mux.Unlock()

	token, err := c.sdk.CreateIAMToken(ctx)
	if err == nil {
		c.expiresAt, err = ptypes.Timestamp(token.GetExpiresAt())
		if err != nil {
			return c.token, err
		}
		c.token = token.GetIamToken()
	}
	return c.token, nil
}
