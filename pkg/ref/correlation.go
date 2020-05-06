package ref

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
)

//
// Labels
const (
	// = Application
	PartOfLabel = "app.kubernetes.io/part-of"
)

var (
	// Application identifier included in correlation labels.
	// **Must set be by the using application.
	Application = ""
)

//
// Build unique correlation label for an object.
func CorrelationLabel(object v1.Object) (label, uid string) {
	label = string(object.GetUID())
	uid = strings.ToLower(ToKind(object))
	return
}

//
// Build correlation labels for an object.
func CorrelationLabels(object v1.Object) map[string]string {
	label, uid := CorrelationLabel(object)
	return map[string]string{
		PartOfLabel: Application,
		label:       uid,
	}
}
