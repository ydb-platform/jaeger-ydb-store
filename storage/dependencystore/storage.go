package dependencystore

import (
	"github.com/jaegertracing/jaeger/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

// DependencyStore handles read/writes dependencies to YDB
type DependencyStore struct {
}

// GetDependencies should return dependency data from YDB, but it's not stored there, so we return nothing
func (DependencyStore) GetDependencies(endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
