package channel

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDoRequestReturnsUpstreamRedirectWithoutFollowing(t *testing.T) {
	service.InitHttpClient()
	gin.SetMode(gin.TestMode)
	sharedClient := service.GetHttpClient()
	require.NotNil(t, sharedClient)
	require.NotNil(t, sharedClient.CheckRedirect)
	originalRedirectPolicy := reflect.ValueOf(sharedClient.CheckRedirect).Pointer()

	var targetRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetRequests.Add(1)
		w.WriteHeader(http.StatusTeapot)
	}))
	defer target.Close()

	const responseBody = "redirect response"
	tests := []int{
		http.StatusMovedPermanently,
		http.StatusFound,
		http.StatusSeeOther,
		http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect,
	}

	for _, statusCode := range tests {
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			targetRequests.Store(0)
			var sourceRequests atomic.Int32
			type sourceResult struct {
				body []byte
				err  error
			}
			sourceResultCh := make(chan sourceResult, 1)
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				sourceRequests.Add(1)
				body, err := io.ReadAll(r.Body)
				sourceResultCh <- sourceResult{body: body, err: err}
				w.Header().Set("Location", target.URL+"/redirect-target")
				w.WriteHeader(statusCode)
				_, _ = io.WriteString(w, responseBody)
			}))
			defer source.Close()

			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodPost, "/relay", nil)

			req, err := http.NewRequest(http.MethodPost, source.URL, bytes.NewReader([]byte("request body")))
			require.NoError(t, err)
			info := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{}}

			resp, err := doRequest(ctx, req, info)
			require.NoError(t, err)
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			gotSource := <-sourceResultCh
			require.NoError(t, gotSource.err)

			assert.Equal(t, statusCode, resp.StatusCode)
			assert.Equal(t, target.URL+"/redirect-target", resp.Header.Get("Location"))
			assert.Equal(t, responseBody, string(body))
			assert.Equal(t, []byte("request body"), gotSource.body)
			assert.EqualValues(t, 1, sourceRequests.Load())
			assert.Zero(t, targetRequests.Load())
		})
	}

	assert.Equal(t, originalRedirectPolicy, reflect.ValueOf(sharedClient.CheckRedirect).Pointer(), "the cached client must not be mutated")
}

func TestSingleAttemptHTTPExecutionNeverReplays(t *testing.T) {
	service.InitHttpClient()
	shared := service.GetHttpClient()
	require.NotNil(t, shared)
	for _, scenario := range []string{"success", "upstream_error", "redirect", "connection_lost"} {
		t.Run(scenario, func(t *testing.T) {
			var calls, redirected atomic.Int32
			protocol := make(chan int, 4)
			upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/target" {
					redirected.Add(1)
					w.WriteHeader(http.StatusTeapot)
					return
				}
				calls.Add(1)
				protocol <- r.ProtoMajor
				_, _ = io.Copy(io.Discard, r.Body)
				switch scenario {
				case "upstream_error":
					w.WriteHeader(http.StatusServiceUnavailable)
				case "redirect":
					w.Header().Set("Location", "https://"+r.Host+"/target")
					w.WriteHeader(http.StatusTemporaryRedirect)
				case "connection_lost":
					hijacker, ok := w.(http.Hijacker)
					if !assert.True(t, ok) {
						return
					}
					conn, _, err := hijacker.Hijack()
					if assert.NoError(t, err) {
						_ = conn.Close()
					}
				default:
					_, _ = io.WriteString(w, "synthetic")
				}
			}))
			upstream.EnableHTTP2 = true
			upstream.StartTLS()
			defer upstream.Close()
			roots := x509.NewCertPool()
			roots.AddCert(upstream.Certificate())
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
			previous := shared.Transport
			shared.Transport = transport
			t.Cleanup(func() { shared.Transport = previous; transport.CloseIdleConnections() })

			info := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{}, SingleAttempt: &relaycommon.SingleAttempt{}}
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request = httptest.NewRequest(http.MethodPost, "/relay", nil)
			req, err := http.NewRequest(http.MethodPost, upstream.URL, bytes.NewReader([]byte("synthetic")))
			require.NoError(t, err)
			req.Header.Set("Idempotency-Key", "synthetic-attempt")
			resp, err := doRequest(ctx, req, info)
			if scenario == "connection_lost" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				_, _ = io.Copy(io.Discard, resp.Body)
				require.NoError(t, resp.Body.Close())
			}
			select {
			case major := <-protocol:
				assert.Equal(t, 1, major, "HTTP/2-capable upstream must receive HTTP/1")
			case <-time.After(2 * time.Second):
				t.Fatal("no upstream HTTP request observed")
			}
			require.NotNil(t, req.GetBody, "caller request must not be mutated")
			second, err := http.NewRequest(http.MethodPost, upstream.URL, bytes.NewReader([]byte("synthetic")))
			require.NoError(t, err)
			_, err = doRequest(ctx, second, info)
			require.ErrorContains(t, err, "already attempted")
			assert.EqualValues(t, 1, calls.Load())
			assert.Zero(t, redirected.Load())
			assert.False(t, transport.DisableKeepAlives, "shared transport must not be mutated")
		})
	}
}
