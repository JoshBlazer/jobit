package tenant

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/sluice/internal/storage"
)

type Tenant = storage.Tenant

var ErrUnauthorized = errors.New("unauthorized")

type contextKey struct{}

// FromContext retrieves the tenant injected by the auth middleware.
func FromContext(ctx context.Context) (*Tenant, bool) {
	t, ok := ctx.Value(contextKey{}).(*Tenant)
	return t, ok && t != nil
}

// WithTenant returns a context carrying the authenticated tenant.
func WithTenant(ctx context.Context, t *Tenant) context.Context {
	return context.WithValue(ctx, contextKey{}, t)
}

// IDFromContext returns the tenant ID or the system fallback UUID.
func IDFromContext(ctx context.Context) uuid.UUID {
	t, ok := FromContext(ctx)
	if !ok {
		return uuid.MustParse("00000000-0000-0000-0000-000000000001")
	}
	return t.ID
}
