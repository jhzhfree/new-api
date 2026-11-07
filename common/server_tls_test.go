package common

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServerTLSConfiguration(t *testing.T) {
	cfg, err := LoadServerTLS("", "")
	require.NoError(t, err)
	require.Nil(t, cfg)
	_, err = LoadServerTLS("missing", "")
	require.Error(t, err)
	_, err = LoadServerTLS("missing", "missing")
	require.Error(t, err)
	fixture := httptest.NewTLSServer(http.NotFoundHandler())
	defer fixture.Close()
	certificate := fixture.TLS.Certificates[0]
	key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	require.NoError(t, err)
	certPath, keyPath := filepath.Join(t.TempDir(), "cert.pem"), filepath.Join(t.TempDir(), "key.pem")
	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]}), 0600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600))
	cfg, err = LoadServerTLS(certPath, keyPath)
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	server.TLS = cfg
	server.StartTLS()
	defer server.Close()
	response, err := server.Client().Get(server.URL)
	require.NoError(t, err)
	response.Body.Close()
	require.Equal(t, 204, response.StatusCode)
}
