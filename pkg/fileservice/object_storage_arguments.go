// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileservice

import (
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type ObjectStorageArguments struct {
	// misc
	Name                 string `toml:"name"`
	KeyPrefix            string `toml:"key-prefix"`
	SharedConfigProfile  string `toml:"shared-config-profile"`
	NoDefaultCredentials bool   `toml:"no-default-credentials"`
	NoBucketValidation   bool   `toml:"no-bucket-validation"`
	Concurrency          int64  `toml:"concurrency"`

	// s3
	Bucket    string   `toml:"bucket"`
	Endpoint  string   `toml:"endpoint"`
	IsMinio   bool     `toml:"is-minio"`
	Region    string   `toml:"region"`
	CertFiles []string `toml:"cert-files"`

	// credentials
	RoleARN         string `json:"-" toml:"role-arn"`
	BearerToken     string `json:"-" toml:"bearer-token"`
	ExternalID      string `json:"-" toml:"external-id"`
	KeyID           string `json:"-" toml:"key-id"`
	KeySecret       string `json:"-" toml:"key-secret"`
	RAMRole         string `json:"-" toml:"ram-role"`
	RoleSessionName string `json:"-" toml:"role-session-name"`
	SecurityToken   string `json:"-" toml:"security-token"`
	SessionToken    string `json:"-" toml:"session-token"`

	// HDFS
	IsHDFS                       bool   `toml:"is-hdfs"`
	User                         string `toml:"user"`
	KerberosServicePrincipleName string `toml:"kerberos-service-principle-name"`
	KerberosUsername             string `toml:"kerberos-username"`
	KerberosRealm                string `toml:"kerberos-realm"`
	KerberosPassword             string `json:"-" toml:"kerberos-password"`
	KerberosKeytabPath           string `toml:"kerberos-keytab-path"`
}

func (o ObjectStorageArguments) String() string {
	bs, err := json.Marshal(o)
	if err != nil {
		panic(err)
	}
	return string(bs)
}

func (o *ObjectStorageArguments) SetFromString(arguments []string) error {
	for _, pair := range arguments {
		key, value, ok := strings.Cut(pair, "=")
		if !ok {
			return moerr.NewInvalidInputNoCtxf("invalid S3 argument: %s", pair)
		}

		switch strings.ToLower(key) {

		case "name":
			o.Name = value
		case "prefix", "key-prefix":
			o.KeyPrefix = value
		case "shared-config-profile":
			o.SharedConfigProfile = value
		case "no-bucket-validation":
			b, err := strconv.ParseBool(value)
			if err == nil {
				o.NoBucketValidation = b
			}
		case "no-default-credentials":
			b, err := strconv.ParseBool(value)
			if err == nil {
				o.NoDefaultCredentials = b
			}
		case "concurrency":
			n, err := strconv.ParseInt(value, 10, 64)
			if err == nil {
				o.Concurrency = n
			}

		case "bucket":
			o.Bucket = value
		case "endpoint":
			o.Endpoint = value
		case "is-minio", "minio":
			o.IsMinio = value != "false" && value != "0"
		case "region":
			o.Region = value
		case "cert-files":
			o.CertFiles = strings.Split(value, ",")

		case "role-arn":
			o.RoleARN = value
		case "bearer-token":
			o.BearerToken = value
		case "external-id":
			o.ExternalID = value
		case "key", "key-id":
			o.KeyID = value
		case "secret", "key-secret", "secret-id":
			o.KeySecret = value
		case "ram-role":
			o.RAMRole = value
		case "role-session-name":
			o.RoleSessionName = value
		case "security-token":
			o.SecurityToken = value
		case "token", "session-token":
			o.SessionToken = value

		case "user":
			o.User = value
		case "is-hdfs":
			o.IsHDFS = value != "false" && value != "0"

		case "kerberos-service-principle-name":
			o.KerberosServicePrincipleName = value
		case "kerberos-username":
			o.KerberosUsername = value
		case "kerberos-realm":
			o.KerberosRealm = value
		case "kerberos-password":
			o.KerberosPassword = value
		case "kerberos-keytab-path":
			o.KerberosKeytabPath = value

		default:
			return moerr.NewInvalidInputNoCtxf("invalid S3 argument: %s", pair)
		}

	}
	return nil
}

var qcloudEndpointPattern = regexp.MustCompile(`cos\.([^.]+)\.myqcloud\.com`)

func (o *ObjectStorageArguments) validate() error {

	// validate endpoint
	var endpointURL *url.URL
	if o.Endpoint != "" {
		var err error
		endpointURL, err = url.Parse(o.Endpoint)
		if err != nil {
			return err
		}
		if endpointURL.Scheme == "" {
			endpointURL.Scheme = "https"
		}
		o.Endpoint = endpointURL.String()
	}

	// region
	if o.Region == "" {

		if o.Endpoint != "" && strings.Contains(o.Endpoint, "myqcloud.com") {
			// 腾讯云
			matches := qcloudEndpointPattern.FindStringSubmatch(o.Endpoint)
			if len(matches) > 0 {
				o.Region = matches[1]
			}

		} else if o.Endpoint != "" && strings.Contains(o.Endpoint, "amazonaws.com") {
			// AWS
			// try to get region from bucket
			resp, err := http.Head("https://" + o.Bucket + ".s3.amazonaws.com")
			if err == nil {
				if value := resp.Header.Get("x-amz-bucket-region"); value != "" {
					o.Region = value
				}
			} else {
				return err
			}
		}

	}

	// role session name
	if o.RoleSessionName == "" {
		o.RoleSessionName = "mo-service"
	}

	return nil
}

func (o *ObjectStorageArguments) shouldLoadDefaultCredentials() bool {

	// default credentials enabled
	if !o.NoDefaultCredentials {
		return true
	}

	// default credentials disabled, but role arn is not empty
	if o.RoleARN != "" {
		return true
	}

	return false
}
