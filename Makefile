GOOS ?= `go env GOOS`
GOBIN ?= ${GOPATH}/bin

# Run tests
test: build
	go test ./pkg/... -coverprofile cover.out
	export LOG_DEVELOPMENT=1;\
		export LOG_LEVEL=3;\
		bin/inventory

# Build.
build: generate fmt vet
	mkdir -p bin
	go build -o bin/inventory github.com/konveyor/controller/pkg/cmd/inventory

# Run go fmt against code
fmt:
	go fmt ./pkg/...

# Run go vet against code
vet:
	go vet -structtag=false ./pkg/...

# Generate code
generate: controller-gen
	${CONTROLLER_GEN} object:headerFile="./hack/boilerplate.go.txt" paths="./pkg/..."

# find or download deepcopy-gen
# download controller-gen if necessary
controller-gen:
ifeq (, $(shell which controller-gen))
	 @{ \
	 set -e ;\
	 CONTROLLER_GEN_TMP_DIR=$$(mktemp -d) ;\
	 cd $$CONTROLLER_GEN_TMP_DIR ;\
	 go mod init tmp ;\
	 go get sigs.k8s.io/controller-tools/cmd/controller-gen@v0.3.0 ;\
	 rm -rf $$CONTROLLER_GEN_TMP_DIR ;\
	}
CONTROLLER_GEN=$(GOBIN)/controller-gen
else
CONTROLLER_GEN=$(shell which controller-gen)
endif
