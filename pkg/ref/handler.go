package ref

import (
	"context"
	"reflect"
	"strings"

	"github.com/konveyor/controller/pkg/logging"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Build an event handler.
// Example:
//
//	err = cnt.Watch(
//	   &source.Kind{
//	      Type: &api.Referenced{},
//	   },
//	   libref.Handler(&api.Owner{}))
func Handler(owner interface{}) handler.EventHandler {
	log := logging.WithName("ref|handler")
	ownerKind := ToKind(owner)
	return handler.EnqueueRequestsFromMapFunc(
		func(cxt context.Context, obj client.Object) []reconcile.Request {
			list := GetRequests(obj, ownerKind)
			if len(list) > 0 {
				log.V(4).Info(
					"handler: request list.",
					"referenced",
					obj.GetObjectKind().GroupVersionKind().Kind,
					"owner",
					ownerKind,
					"list",
					list)
			}
			return list
		},
	)
}

// Impl the handler interface.
func GetRequests(obj client.Object, ownerKind string) []reconcile.Request {
	target := Target{
		Kind:      obj.GetObjectKind().GroupVersionKind().Kind,
		Name:      obj.GetName(),
		Namespace: obj.GetNamespace(),
	}
	list := []reconcile.Request{}
	for _, owner := range Map.Find(target) {
		if owner.Kind != ownerKind {
			continue
		}
		list = append(
			list,
			reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: owner.Namespace,
					Name:      owner.Name,
				},
			})
	}

	return list
}

// Determine the resource Kind.
func ToKind(resource interface{}) string {
	t := reflect.TypeOf(resource).String()
	p := strings.SplitN(t, ".", 2)
	return string(p[len(p)-1])
}
