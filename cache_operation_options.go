package smartcahce

import "time"

const (
	NoCompression CompressionType = iota
	GzipCompression
)

type CompressionType int

type CacheOperationOptions struct {
	RefreshAfter time.Duration
	KeyTTL       time.Duration
	Bypass       bool
	Override     bool
	Compression  CompressionType
	Version      string

	// DataSourceLockTTL defines lock duration for asynchronous item retrieval from a source.
	// Zero means no lock acquired (default), i.e. number of concurrent requests to the source equals the
	// number of processes running versioned-cache otherwise (non-zero) only one request can be sent
	// to the source during this time period.
	DataSourceLockTTL time.Duration

	// ExpirationFn calculates expiration and key expiration based on received item.
	ExpirationFn func(data interface{}) (refreshAfter time.Duration, KeyTTL time.Duration)
}

func (o CacheOperationOptions) Clone() *CacheOperationOptions {
	return &CacheOperationOptions{
		RefreshAfter: o.RefreshAfter,
		KeyTTL:       o.KeyTTL,
		Bypass:       o.Bypass,
		Override:     o.Override,
		Compression:  o.Compression,
	}
}
