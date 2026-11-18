// Copyright 2023 Greenmask
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

package utils

import (
	"net/url"
	"regexp"
	"strings"
)

const hiddenCredential = "***"

// credentialKeywords matches the user and password of a keyword/value connection string, such as
// user=alice password='s3 cret', with the value either single-quoted or running to the next space.
var credentialKeywords = regexp.MustCompile(`(?i)\b((?:user|password)\s*=\s*)('(?:[^'\\]|\\.)*'|\S*)`)

// HideCredentials returns a connection string with its user and password replaced by ***, so it can
// be logged. It handles the URI form (postgresql://user:password@host/db?password=...) and the
// keyword/value form (user=... password=...). A plain database name is returned as it is, and a URI
// that cannot be parsed is hidden whole.
func HideCredentials(dsn string) string {
	if strings.HasPrefix(dsn, "postgresql://") || strings.HasPrefix(dsn, "postgres://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return hiddenCredential
		}
		// Rewrite the query by hand so the other parameters keep their order and escaping.
		if u.RawQuery != "" {
			params := strings.Split(u.RawQuery, "&")
			for i, param := range params {
				key, _, _ := strings.Cut(param, "=")
				if name, err := url.QueryUnescape(key); err == nil &&
					(strings.EqualFold(name, "user") || strings.EqualFold(name, "password")) {
					params[i] = key + "=" + hiddenCredential
				}
			}
			u.RawQuery = strings.Join(params, "&")
		}
		if u.User == nil {
			return u.String()
		}
		// Written in by hand too: url.URL.String would escape the asterisks.
		credentials := hiddenCredential
		if _, ok := u.User.Password(); ok {
			credentials += ":" + hiddenCredential
		}
		u.User = nil
		return strings.Replace(u.String(), "://", "://"+credentials+"@", 1)
	}
	if strings.Contains(dsn, "=") {
		return credentialKeywords.ReplaceAllString(dsn, "${1}"+hiddenCredential)
	}
	return dsn
}

// HideCredentialsInArgs returns a copy of pg_dump or pg_restore arguments with the credentials of
// the --dbname value hidden, for logging. The arguments passed to the command are not changed.
func HideCredentialsInArgs(args []string) []string {
	hidden := make([]string, len(args))
	copy(hidden, args)
	for i, arg := range hidden {
		switch {
		case arg == "--dbname" && i+1 < len(hidden):
			hidden[i+1] = HideCredentials(hidden[i+1])
		case strings.HasPrefix(arg, "--dbname="):
			hidden[i] = "--dbname=" + HideCredentials(strings.TrimPrefix(arg, "--dbname="))
		}
	}
	return hidden
}
