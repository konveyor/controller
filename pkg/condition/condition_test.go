package condition

import (
	"testing"
	"time"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCondition_Equal(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	// Setup
	condA := Condition{
		Type:     "ThingNotFound",
		Status:   True,
		Reason:   "NotFound",
		Category: Error,
		Message:  "Thing not found.",
	}
	condB := Condition{
		Type:     "ThingNotFound",
		Status:   True,
		Reason:   "NotFound",
		Category: Error,
		Message:  "Thing not found.",
	}

	// EqTest
	g.Expect(condA.Equal(condB)).To(gomega.BeTrue())
	// NotEqTest
	condB.Reason = "Changed"
	g.Expect(condA.Equal(condB)).To(gomega.BeFalse())
}

func TestCondition_Update(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Setup
	now := metav1.NewTime(time.Now())
	condA := Condition{
		Type:     "ThingNotFound",
		Status:   True,
		Reason:   "NotFound",
		Category: Error,
		Message:  "Thing not found.",
	}
	condB := Condition{
		Type:     "ThingNotFound",
		Status:   False,
		Reason:   "Found",
		Category: Warn,
		Message:  "Thing not found.",
	}

	// EqTest
	condA.Update(condB)
	LastTransitionTime := condA.LastTransitionTime
	condA.LastTransitionTime = now // for comparison in validation.
	condB.LastTransitionTime = now // for comparison in validation.
	condB.staged = true

	// Validation
	g.Expect(LastTransitionTime).NotTo(gomega.Equal(nil))
	g.Expect(condA.staged).To(gomega.BeTrue())
	g.Expect(condA).To(gomega.Equal(condB))
}

//
// Conditions
//

func TestConditions_BeginStagingConditions(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Setup
	conditions := Conditions{
		List: []Condition{
			{Type: "A", staged: true},
			{Type: "B", staged: true},
			{Type: "C", staged: true},
			{Type: "D", staged: true, Durable: true},
		},
	}

	// Test
	conditions.BeginStagingConditions()

	// Validation
	g.Expect(conditions.staging).To(gomega.BeTrue())
	g.Expect(conditions.List).To(gomega.Equal([]Condition{
		{Type: "A", staged: false},
		{Type: "B", staged: false},
		{Type: "C", staged: false},
		{Type: "D", staged: true, Durable: true},
	}))
}

func TestConditions_EndStagingConditions(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Setup
	conditions := Conditions{
		List: []Condition{
			{Type: "A", staged: false},
			{Type: "B", staged: true},
			{Type: "C", staged: false},
			{Type: "D", staged: true},
		},
	}

	// Test
	conditions.EndStagingConditions()

	// Validation
	g.Expect(conditions.staging).To(gomega.BeFalse())
	g.Expect(conditions.List).To(gomega.Equal([]Condition{
		{Type: "B", staged: true},
		{Type: "D", staged: true},
	}))
}

func TestConditions_SetCondition(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	now := metav1.NewTime(time.Now())

	// Setup
	conditions := Conditions{}
	condition := Condition{
		Type:     "ThingNotFound",
		Status:   True,
		Reason:   "NotFound",
		Category: Error,
		Message:  "Thing not found.",
	}

	// SetTest
	conditions.SetCondition(condition)
	LastTransitionTime := condition.LastTransitionTime
	conditions.List[0].LastTransitionTime = now // for comparison in validation.

	// Validation
	g.Expect(LastTransitionTime).NotTo(gomega.Equal(nil))
	g.Expect(conditions.List).To(
		gomega.Equal([]Condition{
			{
				Type:               "ThingNotFound",
				Status:             True,
				Reason:             "NotFound",
				Category:           Error,
				Message:            "Thing not found.",
				LastTransitionTime: now,
				staged:             true,
			},
		}))

	// UpdateTest - no change.
	conditions.SetCondition(condition)

	// Validation
	g.Expect(len(conditions.List)).To(gomega.Equal(1))
	g.Expect(conditions.List).To(
		gomega.Equal([]Condition{
			{
				Type:               "ThingNotFound",
				Status:             True,
				Reason:             "NotFound",
				Category:           Error,
				Message:            "Thing not found.",
				LastTransitionTime: now,
				staged:             true,
			},
		}))
}

func TestConditions_StageCondition(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Setup
	conditions := Conditions{
		List: []Condition{
			{Type: "A"},
			{Type: "B"},
			{Type: "C"},
			{Type: "D"},
		},
	}

	// Test
	conditions.StageCondition("A", "C", "X")

	// Validation
	g.Expect(conditions.staging).To(gomega.BeFalse())
	g.Expect(conditions.List).To(gomega.Equal([]Condition{
		{Type: "A", staged: true},
		{Type: "B", staged: false},
		{Type: "C", staged: true},
		{Type: "D", staged: false},
	}))
}

func TestConditions_DeleteCondition(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Setup
	conditions := Conditions{
		List: []Condition{
			{Type: "A"},
			{Type: "B"},
			{Type: "C"},
			{Type: "D"},
			{Type: "E"},
		},
	}

	// Test
	conditions.DeleteCondition("B", "D")

	// Validation
	g.Expect(conditions.List).To(gomega.Equal([]Condition{
		{Type: "A"},
		{Type: "C"},
		{Type: "E"},
	}))
}

func TestConditions_DeleteConditionStaging(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Setup
	conditions := Conditions{
		List: []Condition{
			{Type: "A"},
			{Type: "B"},
			{Type: "C"},
			{Type: "D"},
			{Type: "E"},
		},
	}

	// Test
	conditions.BeginStagingConditions()
	conditions.DeleteCondition("B", "D")

	// Validation
	g.Expect(conditions.List).To(gomega.Equal([]Condition{
		{Type: "A"},
		{Type: "B", staged: false},
		{Type: "C"},
		{Type: "D", staged: false},
		{Type: "E"},
	}))
}

func TestConditions_FindCondition(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Setup
	conditions := Conditions{
		List: []Condition{
			{Type: "A"},
			{Type: "B"},
		},
	}

	var nil *Condition

	// Test Found Status: True
	g.Expect(conditions.FindCondition("A")).To(gomega.Equal(&conditions.List[0]))
	// Test NotFound
	g.Expect(conditions.FindCondition("X")).To(gomega.Equal(nil))
}

func TestConditions_FindConditionStaging(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Setup
	conditions := Conditions{
		staging: true,
		List: []Condition{
			{Type: "A", staged: true},
			{Type: "B", staged: false},
		},
	}

	var nil *Condition

	// Test Found Status: True
	g.Expect(conditions.FindCondition("A")).To(gomega.Equal(&conditions.List[0]))
	// Test NotFound
	g.Expect(conditions.FindCondition("B")).To(gomega.Equal(nil))
}

func TestConditions_HasCondition(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Setup
	conditions := Conditions{
		List: []Condition{
			{Type: "A", Status: True},
			{Type: "B", Status: False},
			{Type: "C", Status: True},
		},
	}

	// Test Found Status: True
	g.Expect(conditions.HasCondition("A", "C")).To(gomega.BeTrue())
	// Test NotFound
	g.Expect(conditions.HasCondition("X")).To(gomega.BeFalse())
	// Test Status: not-True
	g.Expect(conditions.HasCondition("B")).To(gomega.BeFalse())
}

func TestConditions_HasAnyCondition(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Setup
	conditions := Conditions{
		List: []Condition{
			{Type: "A", Status: True},
			{Type: "B", Status: False},
		},
	}

	// Test Found Status: True
	g.Expect(conditions.HasAnyCondition("A", "B")).To(gomega.BeTrue())
}

func TestConditions_HasConditionStaging(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Setup
	conditions := Conditions{
		staging: true,
		List: []Condition{
			{Type: "A", Status: True, staged: true},
			{Type: "B", Status: True},
		},
	}

	// Test Staging and staged.
	g.Expect(conditions.HasCondition("A")).To(gomega.BeTrue())
	// Test Staging and not staged.
	g.Expect(conditions.HasCondition("B")).To(gomega.BeFalse())
}

func TestConditions_HasConditionCategory(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Setup
	conditions := Conditions{
		List: []Condition{
			{Type: "A", Category: Error, Status: True},
			{Type: "B", Category: Critical, Status: False},
			{Type: "C", Category: Warn, Status: True},
			{Type: "D", Category: Error, Status: False},
		},
	}

	// Test Found Status: True
	g.Expect(conditions.HasConditionCategory(Error, Warn)).To(gomega.BeTrue())
	// Test NotFound
	g.Expect(conditions.HasConditionCategory("X")).To(gomega.BeFalse())
	// Test Status: not-True
	g.Expect(conditions.HasConditionCategory(Critical)).To(gomega.BeFalse())
}

func TestConditions_HasConditionCategoryStaging(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Setup
	conditions := Conditions{
		staging: true,
		List: []Condition{
			{Type: "A", Category: Error, Status: True, staged: true},
			{Type: "B", Category: Critical, Status: False},
			{Type: "C", Category: Warn, Status: True, staged: true},
			{Type: "D", Category: Error, Status: False},
		},
	}

	// Test Staging and staged.
	g.Expect(conditions.HasConditionCategory(Error, Warn)).To(gomega.BeTrue())
	// Test Staging and not staged.
	g.Expect(conditions.HasConditionCategory(Critical)).To(gomega.BeFalse())
}

func TestConditions_ChangeSet(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	// Setup
	conditions := Conditions{
		List: []Condition{
			{Type: "A"},
			{Type: "B"},
			{Type: "C"},
			{Type: "D"},
		},
	}

	conditions.BeginStagingConditions()
	conditions.SetCondition(Condition{Type: "A"})
	conditions.SetCondition(Condition{Type: "B"})
	conditions.SetCondition(Condition{Type: "C"})
	conditions.SetCondition(Condition{Type: "E"})
	conditions.SetCondition(Condition{Type: "A", Reason: "X"})
	conditions.EndStagingConditions()
	explain := conditions.Explain()

	g.Expect(len(explain.Added)).To(gomega.Equal(1))
	g.Expect(explain.Added["E"].Type).To(gomega.Equal("E"))
	g.Expect(len(explain.Updated)).To(gomega.Equal(1))
	g.Expect(explain.Updated["A"].Type).To(gomega.Equal("A"))
	g.Expect(len(explain.Deleted)).To(gomega.Equal(1))
	g.Expect(explain.Deleted["D"].Type).To(gomega.Equal("D"))
}
