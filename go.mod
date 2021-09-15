module github.com/konveyor/controller

go 1.14

require (
	github.com/appscode/jsonpatch v1.0.1 // indirect
	github.com/evanphx/json-patch v4.5.0+incompatible // indirect
	github.com/gin-contrib/cors v1.3.1
	github.com/gin-gonic/gin v1.7.2
	github.com/go-logr/logr v0.3.0
	github.com/go-logr/zapr v0.3.0
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/google/go-cmp v0.5.2 // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/google/uuid v1.1.2
	github.com/googleapis/gnostic v0.2.0 // indirect
	github.com/gorilla/websocket v1.4.2
	github.com/gregjones/httpcache v0.0.0-20190611155906-901d90724c79 // indirect
	github.com/imdario/mergo v0.3.11 // indirect
	github.com/kr/pretty v0.2.0 // indirect
	github.com/mattn/go-sqlite3 v1.14.4
	github.com/onsi/ginkgo v1.10.2 // indirect
	github.com/onsi/gomega v1.7.0
	github.com/pborman/uuid v1.2.1 // indirect
	github.com/peterbourgon/diskv v2.0.1+incompatible // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.1.0 // indirect
	github.com/stretchr/testify v1.6.1 // indirect
	go.uber.org/atomic v1.4.0 // indirect
	go.uber.org/zap v1.10.0
	golang.org/x/crypto v0.0.0-20201002170205-7f63de1d35b0 // indirect
	golang.org/x/oauth2 v0.0.0-20200902213428-5d25da1a8d43 // indirect
	golang.org/x/sys v0.0.0-20200909081042-eff7692f9009 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	k8s.io/api v0.17.4
	k8s.io/apiextensions-apiserver v0.17.4 // indirect
	k8s.io/apimachinery v0.17.4
	k8s.io/apiserver v0.17.1
	k8s.io/client-go v0.17.4
	sigs.k8s.io/controller-runtime v0.1.11
	sigs.k8s.io/testing_frameworks v0.1.2 // indirect
)

// Use fork
replace bitbucket.org/ww/goautoneg v0.0.0-20120707110453-75cd24fc2f2c => github.com/markusthoemmes/goautoneg v0.0.0-20190713162725-c6008fefa5b1

replace github.com/vmware-tanzu/velero => github.com/konveyor/velero v0.0.0-20201026230312-8bd8ce8744d5

//k8s deps pinning
replace k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20181127025237-2b1284ed4c93

replace k8s.io/client-go => k8s.io/client-go v0.0.0-20181213151034-8d9ed539ba31

replace k8s.io/api => k8s.io/api v0.0.0-20181213150558-05914d821849

replace k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.0.0-20181213153335-0fe22c71c476
