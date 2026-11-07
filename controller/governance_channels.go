package controller

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// GovernanceChannelOrigin permits credential destinations only from an explicit
// deployment allowlist (or the three native public defaults). Saving never probes.
func GovernanceChannelOrigin(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || len(raw) > 2048 || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(raw, "#") || u.RawPath != "" || u.Opaque != "" {
		return false
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && os.Getenv("GOVERNANCE_ALLOW_LOOPBACK_HTTP") == "true" && net.ParseIP(u.Hostname()).IsLoopback()) {
		return false
	}
	origin := u.Scheme + "://" + u.Host
	allowed := "https://api.openai.com,https://api.deepseek.com,https://api.anthropic.com"
	if extra := os.Getenv("GOVERNANCE_CHANNEL_ALLOWED_ORIGINS"); extra != "" {
		allowed += "," + extra
	}
	for candidate := range strings.SplitSeq(allowed, ",") {
		if strings.TrimSpace(candidate) == origin {
			return true
		}
	}
	return false
}

func GovernanceManagedChannels(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Header("X-Aigw-Channel-Management-Version", "1")
	if c.Request.Method == http.MethodGet {
		if value := c.Param("id"); value != "" {
			id, err := strconv.Atoi(value)
			if err != nil || id < 1 || strconv.Itoa(id) != value {
				c.AbortWithStatus(400)
				return
			}
			channel, err := model.GetChannelById(id, true)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				c.AbortWithStatus(404)
				return
			}
			if err != nil {
				c.AbortWithStatus(503)
				return
			}
			c.JSON(200, model.GovernanceChannelPublic(channel))
			return
		}
		if operation := c.Param("operation"); operation != "" {
			if !governanceIdentifier.MatchString(operation) {
				c.AbortWithStatus(400)
				return
			}
			value, err := model.GovernanceChannelReceipt(c.Request.Context(), c.GetInt("id"), operation)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				c.AbortWithStatus(404)
				return
			}
			if err != nil {
				c.AbortWithStatus(503)
				return
			}
			c.JSON(200, value)
			return
		}
		after := 0
		if cursor := c.Param("cursor"); cursor != "" {
			n, err := strconv.Atoi(cursor)
			if err != nil || n < 1 || strconv.Itoa(n) != cursor {
				c.AbortWithStatus(400)
				return
			}
			after = n
		}
		data, err := model.GovernanceChannels(c.Request.Context(), after)
		if err != nil {
			c.AbortWithStatus(503)
			return
		}
		next := ""
		if len(data) == 100 {
			next = strconv.Itoa(data[len(data)-1].ID)
		}
		c.JSON(200, gin.H{"data": data, "next_cursor": next})
		return
	}
	id := 0
	if value := c.Param("id"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || strconv.Itoa(n) != value {
			c.AbortWithStatus(400)
			return
		}
		id = n
	}
	raw, err := io.ReadAll(io.LimitReader(c.Request.Body, (80<<10)+1))
	if err != nil || len(raw) > 80<<10 {
		c.AbortWithStatus(413)
		return
	}
	var fields map[string]common.RawMessage
	if common.Unmarshal(raw, &fields) != nil {
		c.AbortWithStatus(400)
		return
	}
	for key := range fields {
		switch key {
		case "operation_id", "actor_issuer", "actor_subject", "expected_revision", "name", "type", "base_url", "models", "enabled", "api_key":
		default:
			c.AbortWithStatus(400)
			return
		}
	}
	var in model.GovernanceChannelWrite
	if value, ok := fields["enabled"]; !ok || (string(value) != "true" && string(value) != "false") {
		c.AbortWithStatus(400)
		return
	}
	if common.Unmarshal(raw, &in) != nil || !GovernanceChannelOrigin(in.BaseURL) {
		c.AbortWithStatus(400)
		return
	}
	receipt, err := model.ChangeGovernanceChannel(c.Request.Context(), c.GetInt("id"), id, in)
	in.APIKey = nil
	switch {
	case errors.Is(err, model.ErrGovernanceInvalid):
		c.AbortWithStatus(400)
	case errors.Is(err, model.ErrGovernanceConflict):
		c.AbortWithStatus(409)
	case errors.Is(err, gorm.ErrRecordNotFound):
		c.AbortWithStatus(404)
	case err != nil:
		c.AbortWithStatus(503)
	default:
		c.JSON(200, receipt)
	}
}
