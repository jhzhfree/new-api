package channel

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	appconstant "github.com/QuantumNous/new-api/constant"
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

func TestGovernedWireLimitRejectsBeforeDispatch(t *testing.T) {
	service.InitHttpClient()
	for _, body := range []string{`{"max_tokens":9}`, `{"max_tokens":0}`, `{}`, `{"max_completion_tokens":9}`, `{"max_tokens":8}`} {
		t.Run(body, func(t *testing.T) {
			var calls, dispatches atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				payload, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Equal(t, body, string(payload))
				w.WriteHeader(200)
			}))
			defer upstream.Close()
			guard, err := relaycommon.NewDurableSingleAttempt(func() error { dispatches.Add(1); return nil })
			require.NoError(t, err)
			info := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{}, SingleAttempt: guard, GovernanceMaxTokens: 8}
			ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ctx.Request = httptest.NewRequest(http.MethodPost, "/relay", nil)
			req, err := http.NewRequest(http.MethodPost, upstream.URL, bytes.NewBufferString(body))
			require.NoError(t, err)
			response, err := doRequest(ctx, req, info)
			if body == `{"max_tokens":8}` {
				require.NoError(t, err)
				response.Body.Close()
				require.Equal(t, int32(1), calls.Load())
				require.Equal(t, int32(1), dispatches.Load())
			} else {
				require.Error(t, err)
				require.Nil(t, response)
				require.Zero(t, calls.Load())
				require.Zero(t, dispatches.Load())
			}
		})
	}
}

func TestGovernedInputEstimateBeforeDurableDispatch(t *testing.T) {
	service.InitHttpClient()
	original := appconstant.CountToken
	appconstant.CountToken = false
	t.Cleanup(func() { appconstant.CountToken = original })
	const pixel = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC"
	for _, tc := range []struct {
		name    string
		channel int
		body    string
	}{
		{"text", appconstant.ChannelTypeOpenAI, `{"model":"gpt-4o","max_tokens":8,"messages":[{"role":"user","content":"synthetic"}]}`},
		{"tools", appconstant.ChannelTypeDeepSeek, `{"model":"deepseek-chat","max_tokens":8,"messages":[{"role":"user","content":"synthetic"}],"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}]}`},
		{"image", appconstant.ChannelTypeOpenAI, `{"model":"gpt-4o","max_tokens":8,"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,` + pixel + `"}}]}]}`},
		{"claude-image", appconstant.ChannelTypeAnthropic, `{"model":"claude-sonnet-4-5","max_tokens":8,"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + pixel + `"}}]}]}`},
	} {
		for _, ceiling := range []uint{1, 2000} {
			t.Run(tc.name+"/"+strconv.FormatUint(uint64(ceiling), 10), func(t *testing.T) {
				var calls, dispatches atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					raw, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					assert.Equal(t, tc.body, string(raw))
					w.WriteHeader(200)
				}))
				defer upstream.Close()
				guard, err := relaycommon.NewDurableSingleAttempt(func() error { dispatches.Add(1); return nil })
				require.NoError(t, err)
				info := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{ChannelType: tc.channel}, SingleAttempt: guard, GovernanceMaxTokens: 8, GovernanceMaxInputTokens: ceiling}
				ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
				ctx.Request = httptest.NewRequest("POST", "/relay", nil)
				req, err := http.NewRequest("POST", upstream.URL, bytes.NewBufferString(tc.body))
				require.NoError(t, err)
				res, err := doRequest(ctx, req, info)
				if ceiling == 1 {
					require.Error(t, err)
					require.Nil(t, res)
					assert.Zero(t, calls.Load())
					assert.Zero(t, dispatches.Load())
				} else {
					require.NoError(t, err)
					res.Body.Close()
					assert.Equal(t, int32(1), calls.Load())
					assert.Equal(t, int32(1), dispatches.Load())
				}
			})
		}
	}
	// Local estimation must not fetch external media even if called directly.
	_, err := relaycommon.GovernedInputEstimate([]byte(`{"model":"gpt-4o","max_tokens":8,"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"http://127.0.0.1/private"}}]}]}`), appconstant.ChannelTypeOpenAI)
	require.Error(t, err)
}
