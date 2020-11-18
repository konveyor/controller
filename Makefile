GOOS ?= `go env GOOS`

# Run tests
test: generate fmt vet
	go test ./pkg/... -coverprofile cover.out

# Run go fmt against code
fmt:
	go fmt ./pkg/...

# Run go vet against code
vet:
	go vet ./pkg/...

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
