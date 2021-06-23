package filebacked

import (
	"github.com/onsi/gomega"
	"testing"
)

func TestList(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	type Ref struct {
		ID string
	}

	type Person struct {
		ID   int
		Name string
		Age  int
		List []string
		Ref  []Ref
	}

	type User struct {
		ID   int
		Name string
	}

	input := []interface{}{}
	for i := 0; i < 10; i++ {
		input = append(
			input,
			&Person{
				ID:   i,
				Name: "Elmer",
				Age:  i + 10,
				List: []string{"A", "B"},
				Ref:  []Ref{{"id0"}},
			})
		input = append(
			input,
			User{
				ID:   i,
				Name: "john",
			})
	}

	cat := &catalog

	list := NewList()

	// append
	for i := 0; i < len(input); i++ {
		list.Append(input[i])
	}
	g.Expect(len(cat.content)).To(gomega.Equal(2))
	g.Expect(list.writer.length).To(gomega.Equal(uint64(len(input))))
	g.Expect(list.Len()).To(gomega.Equal(len(input)))

	// iterate
	itr := list.Iter()
	g.Expect(itr.Len()).To(gomega.Equal(len(input)))
	for i := 0; i < len(input); i++ {
		object, hasNext := itr.Next()
		g.Expect(object).ToNot(gomega.BeNil())
		g.Expect(hasNext).To(gomega.BeTrue())
		g.Expect(itr.Len()).To(gomega.Equal(len(input)))
	}

	n := 0
	itr = list.Iter()
	for {
		object, hasNext := itr.Next()
		if hasNext {
			n++
		} else {
			break
		}
		g.Expect(object).ToNot(gomega.BeNil())
		g.Expect(hasNext).To(gomega.BeTrue())
	}
	g.Expect(n).To(gomega.Equal(len(input)))

	n = 0
	itr = list.Iter()
	for {
		person := &Person{}
		hasNext := itr.NextWith(person)
		if hasNext {
			n++
		} else {
			break
		}
		user := &User{}
		hasNext = itr.NextWith(user)
		if hasNext {
			n++
		} else {
			break
		}
		g.Expect(person).ToNot(gomega.BeNil())
		g.Expect(user).ToNot(gomega.BeNil())
		g.Expect(hasNext).To(gomega.BeTrue())
	}
	g.Expect(n).To(gomega.Equal(len(input)))

}
