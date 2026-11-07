package common

import (
	"crypto/tls"
	"errors"
)

// LoadServerTLS enables direct TLS without trusting caller-supplied proxy headers.
// Empty paths retain the existing plaintext listener; a partial pair fails closed.
func LoadServerTLS(certPath, keyPath string) (*tls.Config, error) {
	if certPath == "" && keyPath == "" {
		return nil, nil
	}
	if certPath == "" || keyPath == "" {
		return nil, errors.New("both server TLS certificate and key are required")
	}
	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, errors.New("server TLS certificate configuration invalid")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}}, nil
}
