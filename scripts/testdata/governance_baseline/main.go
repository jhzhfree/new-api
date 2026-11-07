// Build this fixture while the working directory is the pinned baseline module;
// its imports then use that version's real startup migrations. Never serve HTTP.
package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	sqlmysql "github.com/go-sql-driver/mysql"
)

func main() {
	dsn := os.Getenv("SQL_DSN")
	sqlitePath := os.Getenv("GOVERNANCE_SQLITE_PATH")
	allowed := false
	if dsn == "local" {
		relative, err := filepath.Rel(os.TempDir(), sqlitePath)
		allowed = err == nil && filepath.IsAbs(sqlitePath) && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && filepath.Base(sqlitePath) == "audit.db"
	} else if strings.HasPrefix(dsn, "postgres") {
		u, err := url.Parse(dsn)
		allowed = err == nil && net.ParseIP(u.Hostname()).IsLoopback() && strings.HasPrefix(u.Path, "/newapi_audit_")
	} else if cfg, err := sqlmysql.ParseDSN(dsn); err == nil && cfg.Net == "tcp" {
		host, _, err := net.SplitHostPort(cfg.Addr)
		allowed = err == nil && net.ParseIP(host).IsLoopback() && strings.HasPrefix(cfg.DBName, "newapi_audit_")
	}
	if !allowed {
		os.Exit(10)
	}
	common.IsMasterNode = true
	common.SQLitePath = sqlitePath
	if err := model.InitDB(); err != nil {
		os.Exit(1)
	}
	if err := model.DB.Create(&model.Option{Key: "governance_upgrade_sentinel", Value: "preserve-this-value"}).Error; err != nil {
		os.Exit(2)
	}
	user := model.User{Username: "gov-upgrade", Status: common.UserStatusDisabled, Quota: 54321}
	if model.DB.Create(&user).Error != nil {
		os.Exit(5)
	}
	key := make([]byte, 24)
	if _, err := rand.Read(key); err != nil {
		os.Exit(6)
	}
	token := model.Token{UserId: user.Id, Name: "gov-upgrade", Key: hex.EncodeToString(key), Status: common.TokenStatusDisabled, RemainQuota: 12345, UsedQuota: 321}
	if model.DB.Create(&token).Error != nil {
		os.Exit(7)
	}
	digest := sha256.Sum256([]byte(token.Key))
	manifest, err := common.Marshal(map[string]any{"user_id": user.Id, "token_id": token.Id, "key_digest": hex.EncodeToString(digest[:])})
	if err != nil || model.DB.Create(&model.Option{Key: "governance_upgrade_manifest", Value: string(manifest)}).Error != nil {
		os.Exit(8)
	}
	db, err := model.DB.DB()
	if err != nil {
		os.Exit(3)
	}
	if db.Close() != nil {
		os.Exit(4)
	}
}
