// Isolated integration fixture using the actual router, authentication and relay.
package main

import (
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/router"
	"github.com/QuantumNous/new-api/setting/ratio_setting"
	"github.com/gin-gonic/gin"
)

func main() {
	if i18n.Init() != nil {
		os.Exit(13)
	}
	root := os.Getenv("GOVERNANCE_FIXTURE_DIR")
	rel, err := filepath.Rel(os.TempDir(), root)
	if err != nil || !filepath.IsAbs(root) || rel == ".." || strings.HasPrefix(rel, "../") {
		os.Exit(10)
	}
	upstream, err := url.Parse(os.Getenv("GOVERNANCE_FIXTURE_UPSTREAM"))
	if err != nil || upstream.Scheme != "http" || !net.ParseIP(upstream.Hostname()).IsLoopback() {
		os.Exit(11)
	}
	_ = os.Setenv("SQL_DSN", "local")
	_ = os.Setenv("LOG_SQL_DSN", "")
	_ = os.Setenv("GOVERNANCE_SERVICE_USER_ID", "7")
	_ = os.Setenv("GOVERNANCE_ALLOW_LOOPBACK_HTTP", "true")
	// Preserve the production SQLite WAL, busy timeout and immediate write lock.
	common.SQLitePath = filepath.Join(root, "audit.db") + "?_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_txlock=immediate"
	common.IsMasterNode = true
	constant.StreamingTimeout = 30
	common.RetryTimes = 3
	common.RedisEnabled = false
	common.MemoryCacheEnabled = false
	if model.InitDB() != nil {
		os.Exit(1)
	}
	model.LOG_DB = model.DB
	if model.DB.AutoMigrate(&model.Log{}) != nil {
		os.Exit(2)
	}
	if ratio_setting.UpdateModelRatioByJSONString(`{"governance-model":1}`) != nil || ratio_setting.UpdateGroupRatioByJSONString(`{"default":1}`) != nil {
		os.Exit(3)
	}
	var count int64
	if model.DB.Model(&model.User{}).Where("id = ?", 7).Count(&count).Error != nil {
		os.Exit(4)
	}
	if count == 0 {
		user := model.User{Id: 7, Username: "governance-fixture", Status: common.UserStatusEnabled, Group: "default", Quota: 100000000, Setting: `{"billing_preference":"wallet_only"}`}
		token := model.Token{UserId: 7, Key: "syntheticgovernancefixture", Status: common.TokenStatusEnabled, ExpiredTime: -1, UnlimitedQuota: true}
		base := upstream.String()
		channelType := constant.ChannelTypeOpenAI
		switch os.Getenv("GOVERNANCE_FIXTURE_PROTOCOL") {
		case "", "openai", "embedding":
		case "deepseek":
			channelType = constant.ChannelTypeDeepSeek
		case "anthropic":
			channelType = constant.ChannelTypeAnthropic
		default:
			os.Exit(13)
		}
		channel := model.Channel{Name: "synthetic", Type: channelType, Key: "syntheticvendor", Status: common.ChannelStatusEnabled, Group: "default", Models: "governance-model", BaseURL: &base}
		if mode := os.Getenv("GOVERNANCE_FIXTURE_MULTIKEY"); mode != "" {
			if mode != "random" && mode != "polling" {
				os.Exit(14)
			}
			channel.Key = "disabled-synthetic\nsyntheticvendor-one\nsyntheticvendor-two"
			channel.ChannelInfo = model.ChannelInfo{IsMultiKey: true, MultiKeySize: 3, MultiKeyMode: constant.MultiKeyMode(mode), MultiKeyStatusList: map[int]int{0: common.ChannelStatusManuallyDisabled}}
		}
		if model.DB.Create(&user).Error != nil || model.DB.Create(&token).Error != nil || model.DB.Create(&channel).Error != nil {
			os.Exit(5)
		}
		if model.DB.Create(&model.Ability{ChannelId: channel.Id, Model: "governance-model", Group: "default", Enabled: true}).Error != nil {
			os.Exit(6)
		}
	}
	listen := os.Getenv("GOVERNANCE_FIXTURE_LISTEN")
	if listen == "" {
		listen = "127.0.0.1:0"
	}
	host, _, err := net.SplitHostPort(listen)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		os.Exit(12)
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		os.Exit(7)
	}
	engine := gin.New()
	engine.Use(gin.Recovery())
	router.SetRelayRouter(engine)
	if os.WriteFile(filepath.Join(root, "address"), []byte("http://"+listener.Addr().String()), 0600) != nil {
		os.Exit(8)
	}
	if http.Serve(listener, engine) != nil {
		os.Exit(9)
	}
}
