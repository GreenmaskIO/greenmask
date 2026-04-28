// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package models

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	sqldriver "github.com/go-sql-driver/mysql"

	commonconfig "github.com/greenmaskio/greenmask/pkg/common/config"
)

type ConnConfig struct {
	Host             string
	Port             int
	Socket           string
	User             string
	Password         string
	Database         string
	Timeout          time.Duration
	MaxAllowedPacket int
	SSLMode          commonconfig.SSLMode
	TLSConfig        *tls.Config
}

func (d *ConnConfig) Address() string {
	if d.Socket != "" {
		return d.Socket
	}
	return fmt.Sprintf("%s:%d", d.Host, d.Port)
}

// tlsRegistryKey returns a stable key for RegisterTLSConfig based on this config.
func (d *ConnConfig) tlsRegistryKey() string {
	return fmt.Sprintf("greenmask-%s-%s-%d", d.Host, d.Database, d.Port)
}

// URI builds the go-sql-driver DSN string, including socket, TLS, and maxAllowedPacket params.
// For VERIFY_CA / VERIFY_IDENTITY it registers the TLS config with the driver before returning.
func (d *ConnConfig) URI() (string, error) {
	var network, addr string
	if d.Socket != "" {
		network = "unix"
		addr = d.Socket
	} else {
		network = "tcp"
		addr = fmt.Sprintf("%s:%d", d.Host, d.Port)
	}

	base := fmt.Sprintf("%s:%s@%s(%s)/%s", d.User, d.Password, network, addr, d.Database)

	var params []string
	if d.MaxAllowedPacket > 0 {
		params = append(params, fmt.Sprintf("maxAllowedPacket=%d", d.MaxAllowedPacket))
	}

	tlsParam, hasTLS, err := d.dsnTLSParam()
	if err != nil {
		return "", fmt.Errorf("register TLS config: %w", err)
	}
	if hasTLS {
		params = append(params, "tls="+tlsParam)
	}

	if len(params) > 0 {
		base += "?" + strings.Join(params, "&")
	}

	return base, nil
}

// dsnTLSParam returns the go-sql-driver DSN tls= value for this config.
// For VERIFY_CA / VERIFY_IDENTITY with a custom *tls.Config it calls RegisterTLSConfig.
// go-sql-driver's RegisterTLSConfig is internally backed by a sync.Map, so no extra locking is needed.
func (d *ConnConfig) dsnTLSParam() (param string, add bool, err error) {
	switch d.SSLMode {
	case commonconfig.SSLModeDisabled:
		return "false", true, nil
	case commonconfig.SSLModePreferred:
		return "preferred", true, nil
	case commonconfig.SSLModeRequired:
		return "skip-verify", true, nil
	case commonconfig.SSLModeVerifyCA, commonconfig.SSLModeVerifyIdentity:
		if d.TLSConfig == nil {
			return "true", true, nil
		}
		key := d.tlsRegistryKey()
		if err := sqldriver.RegisterTLSConfig(key, d.TLSConfig); err != nil {
			return "", false, fmt.Errorf("RegisterTLSConfig(%q): %w", key, err)
		}
		return key, true, nil
	default:
		return "", false, nil
	}
}
