package fbq

import (
	"github.com/onsi/gomega"
	"testing"
)

func TestQueue(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	type Person struct {
		Name string
		Age  int
	}
	type User struct {
		ID  int
		UID string
	}
	input := []interface{}{}
	for i := 0; i < 10; i++ {
		input = append(
			input,
			Person{
				Name: "Elmer",
				Age:  i + 10,
			})
		input = append(
			input,
			User{
				ID:  i,
				UID: "ABCDE",
			})
	}

	q := New("/tmp")
	defer q.Close()

	for i := 0; i < len(input); i++ {
		err := q.Put(input[i])
		g.Expect(err).To(gomega.BeNil())
	}
	for i := 0; i < len(input); i++ {
		object, hasNext, err := q.Next()
		g.Expect(object).ToNot(gomega.BeNil())
		g.Expect(err).To(gomega.BeNil())
		g.Expect(hasNext).To(gomega.BeTrue())
	}
	itr := q.Iterator()
	defer itr.Close()
	for i := 0; i < len(input); i++ {
		object, hasNext, err := itr.Next()
		g.Expect(object).ToNot(gomega.BeNil())
		g.Expect(err).To(gomega.BeNil())
		g.Expect(hasNext).To(gomega.BeTrue())
	}

	itr = q.Iterator()
	g.Expect(itr.Error()).To(gomega.BeNil())
	defer itr.Close()
	for {
		object, hasNext, err := itr.Next()
		if !hasNext {
			break
		}
		g.Expect(object).ToNot(gomega.BeNil())
		g.Expect(err).To(gomega.BeNil())
		g.Expect(hasNext).To(gomega.BeTrue())
	}

	itr = q.Iterator()
	g.Expect(itr.Error()).To(gomega.BeNil())
	defer itr.Close()
	for {
		person := &Person{}
		hasNext, err := itr.NextWith(person)
		if !hasNext {
			break
		}
		user := &User{}
		hasNext, err = itr.NextWith(user)
		if !hasNext {
			break
		}
		g.Expect(person).ToNot(gomega.BeNil())
		g.Expect(user).ToNot(gomega.BeNil())
		g.Expect(err).To(gomega.BeNil())
		g.Expect(hasNext).To(gomega.BeTrue())
	}
}
