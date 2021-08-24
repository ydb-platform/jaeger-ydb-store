package db

import (
	"context"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
)

type Checkable interface {
	Check(err error) (m ydb.RetryMode)
}

// Retryer contains logic of retrying operations failed with retryable errors.
type Retryer struct {
	// SessionProvider is an interface capable for management of ydb sessions.
	// SessionProvider must not be nil.
	SessionProvider table.SessionProvider

	// MaxRetries is a number of maximum attempts to retry a failed operation.
	// If MaxRetries is zero then no attempts will be made.
	MaxRetries int

	// RetryChecker contains options of mapping errors to retry mode.
	//
	// Note that if RetryChecker's RetryNotFound field is set to true, creation
	// of prepared statements must always be included in the Operation logic.
	// Otherwise when prepared statement become removed by any reason from the
	// server, Retryer will just repeat MaxRetries times reception of statement
	// not found error.
	RetryChecker Checkable

	// Backoff is a selected backoff policy.
	// If backoff is nil, then the DefaultBackoff is used.
	Backoff ydb.Backoff
}

var genericCheck = &checkGeneric{base: &ydb.DefaultRetryChecker}

func RetryGeneric(ctx context.Context, s table.SessionProvider, op table.Operation) error {
	return (Retryer{
		SessionProvider: s,
		MaxRetries:      12,
		RetryChecker:    genericCheck,
		Backoff: ydb.BackoffFunc(func(n int) <-chan time.Time {
			return time.After(time.Second / 4)
		}),
	}).Do(ctx, op)
}

// Do calls op.Do until it return nil or not retriable error.
func (r Retryer) Do(ctx context.Context, op table.Operation) (err error) {
	var (
		s *table.Session
		m ydb.RetryMode
	)
	defer func() {
		if s != nil {
			r.SessionProvider.Put(context.Background(), s)
		}
	}()
	for i := 0; i < r.MaxRetries; i++ {
		if s == nil {
			var e error
			s, e = r.SessionProvider.Get(ctx)
			if e != nil {
				if err == nil {
					// It is initial attempt to get a Session.
					// Otherwise s could be nil only when status bad session
					// received – that is, we must return bad session error to
					// make it possible to lay on for the client.
					err = e
				}
				return
			}
		}
		if err = op.Do(ctx, s); err == nil {
			return nil
		}
		m = r.RetryChecker.Check(err)
		switch {
		case m.MustDeleteSession():
			defer s.Close(ctx)
			s = nil

		case m.MustCheckSession():
			r.SessionProvider.PutBusy(ctx, s)
			s = nil
		}
		if !m.Retriable() {
			return err
		}
		if m.MustBackoff() {
			if e := ydb.WaitBackoff(ctx, r.Backoff, i); e != nil {
				// Return original error to make it possible to lay on for the
				// client.
				return err
			}
		}
	}
	return err
}
