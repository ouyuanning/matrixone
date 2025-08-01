module github.com/matrixorigin/matrixone

// Minimum Go version required
go 1.24.3

require (
	github.com/BurntSushi/toml v1.2.1
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/FastFilter/xorfilter v0.1.4
	github.com/K-Phoen/grabana v0.21.19
	github.com/K-Phoen/sdk v0.12.3
	github.com/KimMachineGun/automemlimit v0.6.0
	github.com/RoaringBitmap/roaring v1.2.3
	github.com/aliyun/alibaba-cloud-sdk-go v1.63.34
	github.com/aliyun/aliyun-oss-go-sdk v3.0.2+incompatible
	github.com/aliyun/credentials-go v1.3.10
	github.com/aws/aws-sdk-go-v2 v1.32.5
	github.com/aws/aws-sdk-go-v2/config v1.28.5
	github.com/aws/aws-sdk-go-v2/credentials v1.17.46
	github.com/aws/aws-sdk-go-v2/service/s3 v1.68.0
	github.com/aws/aws-sdk-go-v2/service/sts v1.33.1
	github.com/aws/smithy-go v1.22.1
	github.com/axiomhq/hyperloglog v0.0.0-20230201085229-3ddf4bad03dc
	github.com/buger/jsonparser v1.1.1
	github.com/cakturk/go-netstat v0.0.0-20200220111822-e5b49efee7a5
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/cockroachdb/errors v1.9.1
	github.com/colinmarc/hdfs/v2 v2.4.0
	github.com/confluentinc/confluent-kafka-go/v2 v2.4.0
	github.com/containerd/cgroups/v3 v3.0.1
	github.com/cpegeric/pdftotext-go v0.0.0-20241112123704-49cb86a3790e
	github.com/detailyang/go-fallocate v0.0.0-20180908115635-432fa640bd2e
	github.com/docker/go-units v0.5.0
	github.com/dolthub/maphash v0.1.0
	github.com/dslipak/pdf v0.0.2
	github.com/elastic/gosigar v0.14.2
	github.com/extism/go-sdk v1.6.0
	github.com/fagongzi/goetty/v2 v2.0.3-0.20230628075727-26c9a2fd5fb8
	github.com/fagongzi/util v0.0.0-20210923134909-bccc37b5040d
	github.com/felixge/fgprof v0.9.6-0.20240831122612-49987e680f04
	github.com/go-sql-driver/mysql v1.8.1
	github.com/gofrs/flock v0.8.1
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/google/btree v1.1.2
	github.com/google/gofuzz v1.2.0
	github.com/google/gops v0.3.25
	github.com/google/pprof v0.0.0-20240625030939-27f56978b8b0
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/google/uuid v1.6.0
	github.com/hashicorp/memberlist v0.3.1
	github.com/hayageek/threadsafe v1.0.1
	github.com/itchyny/gojq v0.12.16
	github.com/jcmturner/gokrb5/v8 v8.4.4
	github.com/jhump/protoreflect v1.15.2
	github.com/jonboulle/clockwork v0.4.0
	github.com/json-iterator/go v1.1.12
	github.com/lni/dragonboat/v4 v4.0.0-20220815145555-6f622e8bcbef
	github.com/lni/goutils v1.3.1-0.20220604063047-388d67b4dbc4
	github.com/lni/vfs v0.2.1-0.20220616104132-8852fd867376
	github.com/matrixorigin/mysql v1.8.2-0.20241106110439-6ac9ee94770d
	github.com/matrixorigin/simdcsv v0.0.0-20230210060146-09b8e45209dd
	github.com/minio/minio-go/v7 v7.0.78
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826
	github.com/ncruces/go-dns v1.2.5
	github.com/openacid/slimarray v0.1.3
	github.com/panjf2000/ants/v2 v2.7.4
	github.com/parquet-go/parquet-go v0.23.0
	github.com/petermattis/goid v0.0.0-20241025130422-66cb2e6d7274
	github.com/pierrec/lz4/v4 v4.1.21
	github.com/pkg/errors v0.9.1
	github.com/plar/go-adaptive-radix-tree v1.0.5
	github.com/prashantv/gostub v1.1.0
	github.com/prometheus/client_golang v1.17.0
	github.com/prometheus/client_model v0.4.1-0.20230718164431-9a2bf3000d16
	github.com/robfig/cron/v3 v3.0.1
	github.com/samber/lo v1.38.1
	github.com/segmentio/encoding v0.4.0
	github.com/shirou/gopsutil/v3 v3.23.12
	github.com/smartystreets/goconvey v1.7.2
	github.com/spf13/cobra v1.8.0
	github.com/spkg/bom v1.0.0
	github.com/stretchr/testify v1.9.0
	github.com/syncthing/notify v0.0.0-20250528144937-c7027d4f7465
	github.com/tencentyun/cos-go-sdk-v5 v0.7.55
	github.com/ti-mo/conntrack v0.5.1
	github.com/ti-mo/netfilter v0.5.2
	github.com/tidwall/btree v1.7.0
	github.com/tidwall/pretty v1.2.1
	github.com/unum-cloud/usearch/golang v0.0.0-20250207215718-306d6646b8f5
	go.starlark.net v0.0.0-20250701195324-d457b4515e0e
	go.uber.org/automaxprocs v1.5.3
	go.uber.org/ratelimit v0.2.0
	go.uber.org/zap v1.24.0
	golang.org/x/exp v0.0.0-20241009180824-f66d83c29e7c
	golang.org/x/sync v0.8.0
	golang.org/x/sys v0.26.0
	gonum.org/v1/gonum v0.14.0
	google.golang.org/grpc v1.65.0
	google.golang.org/protobuf v1.36.0
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/Masterminds/semver/v3 v3.2.1 // indirect
	github.com/andybalholm/brotli v1.1.0 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cilium/ebpf v0.9.1 // indirect
	github.com/clbanning/mxj v1.8.4 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/dylibso/observe-sdk/go v0.0.0-20240819160327-2d926c5d788a // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/goccy/go-json v0.10.3 // indirect
	github.com/godbus/dbus/v5 v5.0.4 // indirect
	github.com/golang-collections/collections v0.0.0-20130729185459-604e922904d3 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/gosimple/slug v1.13.1 // indirect
	github.com/gosimple/unidecode v1.0.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/ianlancetaylor/demangle v0.0.0-20240805132620-81f5be970eca // indirect
	github.com/itchyny/timefmt-go v0.1.6 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/native v1.1.0 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/mdlayher/netlink v1.7.2 // indirect
	github.com/mdlayher/socket v0.5.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mozillazg/go-httpheader v0.2.1 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/opencontainers/runtime-spec v1.0.2 // indirect
	github.com/opentracing/opentracing-go v1.2.1-0.20220228012449-10b1cf09e00b // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/segmentio/asm v1.1.3 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/tetratelabs/wabin v0.0.0-20230304001439-f6f874872834 // indirect
	github.com/tetratelabs/wazero v1.8.1-0.20240916092830-1353ca24fef0 // indirect
	go.opentelemetry.io/proto/otlp v1.3.1 // indirect
	golang.org/x/crypto v0.28.0 // indirect
	golang.org/x/net v0.30.0 // indirect
	golang.org/x/text v0.19.0 // indirect
	golang.org/x/time v0.3.0 // indirect
)

require (
	github.com/DataDog/zstd v1.5.0 // indirect
	github.com/VictoriaMetrics/metrics v1.18.1 // indirect
	github.com/alibabacloud-go/debug v1.0.1 // indirect
	github.com/alibabacloud-go/tea v1.2.2 // indirect
	github.com/andres-erbsen/clock v0.0.0-20160526145045-9e14626cd129 // indirect
	github.com/armon/go-metrics v0.0.0-20180917152333-f0300d1749da // indirect
	github.com/aws/aws-sdk-go v1.55.5
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.7 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.24 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.24 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.24 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.4.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.24.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.28.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.8.0 // indirect
	github.com/bufbuild/protocompile v0.6.0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/pebble v0.0.0-20220407171941-2120d145e292 // indirect
	github.com/cockroachdb/redact v1.1.3 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-metro v0.0.0-20180109044635-280f6062b5bc // indirect
	github.com/getsentry/sentry-go v0.12.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/gopherjs/gopherjs v1.12.80 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.3 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.0 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/klauspost/cpuid/v2 v2.2.8 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/miekg/dns v1.1.53 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/pingcap/errors v0.11.5-0.20201029093017-5a7df2af2ac7 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/common v0.44.0 // indirect
	github.com/prometheus/procfs v0.11.1 // indirect
	github.com/rogpeppe/go-internal v1.10.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/smartystreets/assertions v1.13.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.3 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/mod v0.21.0 // indirect
	golang.org/x/tools v0.26.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240814211410-ddb44dafa142 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// required until memberlist issue 272 is resolved
// see https://github.com/hashicorp/memberlist/pull/273 for progress
replace github.com/hashicorp/memberlist => github.com/matrixorigin/memberlist v0.5.1-0.20230322082342-95015c95ee76

replace (
	github.com/elastic/gosigar v0.14.2 => github.com/matrixorigin/gosigar v0.14.3-0.20241204071856-40aab500bfac
	github.com/fagongzi/goetty/v2 v2.0.3-0.20230628075727-26c9a2fd5fb8 => github.com/matrixorigin/goetty/v2 v2.0.0-20240611082008-a4de209fff3d
	github.com/lni/dragonboat/v4 v4.0.0-20220815145555-6f622e8bcbef => github.com/matrixorigin/dragonboat/v4 v4.0.0-20241019050137-1c6138e9cf8b
	github.com/lni/goutils v1.3.1-0.20220604063047-388d67b4dbc4 => github.com/matrixorigin/goutils v1.3.1-0.20220604063047-388d67b4dbc4
	github.com/lni/vfs v0.2.1-0.20220616104132-8852fd867376 => github.com/matrixorigin/vfs v0.2.1-0.20220616104132-8852fd867376
)
