package filebacked

import (
	"github.com/onsi/gomega"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestMain(m *testing.M) {
	clean := func() {
		d, err := os.Open(WorkingDir)
		if err != nil {
			return
		}
		defer d.Close()
		names, err := d.Readdirnames(-1)
		if err != nil {
			return
		}
		for _, name := range names {
			if filepath.Ext(name) != Extension {
				continue
			}
			err = os.Remove(filepath.Join(WorkingDir, name))
			if err != nil {
				return
			}
		}
	}
	clean()
	m.Run()
	clean()
}

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

	fileExists := func(path string) bool {
		_, err := os.Stat(path)
		return !os.IsNotExist(err)
	}

	cat := &catalog

	list := List{}
	defer list.Close()

	// append
	for i := 0; i < len(input); i++ {
		err := list.Append(input[i])
		g.Expect(err).To(gomega.BeNil())
	}
	g.Expect(len(cat.content)).To(gomega.Equal(2))
	g.Expect(list.writer.length).To(gomega.Equal(uint64(len(input))))
	g.Expect(list.Len()).To(gomega.Equal(len(input)))

	// iterate
	itr := list.Iter()
	g.Expect(itr.Len()).To(gomega.Equal(len(input)))
	for i := 0; i < len(input); i++ {
		object, hasNext, err := itr.Next()
		g.Expect(err).To(gomega.BeNil())
		g.Expect(object).ToNot(gomega.BeNil())
		g.Expect(hasNext).To(gomega.BeTrue())
		g.Expect(itr.Len()).To(gomega.Equal(len(input)))
	}
	itr.Close()
	g.Expect(fileExists(itr.(*Reader).path)).To(gomega.BeFalse())

	n := 0
	itr = list.Iter()
	g.Expect(itr.Error()).To(gomega.BeNil())
	for {
		object, hasNext, err := itr.Next()
		g.Expect(err).To(gomega.BeNil())
		if hasNext {
			n++
		} else {
			break
		}
		g.Expect(object).ToNot(gomega.BeNil())
		g.Expect(err).To(gomega.BeNil())
		g.Expect(hasNext).To(gomega.BeTrue())
	}
	g.Expect(n).To(gomega.Equal(len(input)))
	itr.Close()
	g.Expect(fileExists(itr.(*Reader).path)).To(gomega.BeFalse())

	n = 0
	itr = list.Iter()
	itr2 := list.Iter()
	itr3 := list.Iter()
	g.Expect(itr.Error()).To(gomega.BeNil())
	for {
		person := &Person{}
		hasNext, err := itr.NextWith(person)
		g.Expect(err).To(gomega.BeNil())
		if hasNext {
			n++
		} else {
			break
		}
		user := &User{}
		hasNext, err = itr.NextWith(user)
		g.Expect(err).To(gomega.BeNil())
		if hasNext {
			n++
		} else {
			break
		}
		g.Expect(person).ToNot(gomega.BeNil())
		g.Expect(user).ToNot(gomega.BeNil())
		g.Expect(err).To(gomega.BeNil())
		g.Expect(hasNext).To(gomega.BeTrue())
	}
	g.Expect(n).To(gomega.Equal(len(input)))
	itr.Close()
	itr2.Close()
	itr3.Close()
	g.Expect(fileExists(itr.(*Reader).path)).To(gomega.BeFalse())
	g.Expect(fileExists(itr2.(*Reader).path)).To(gomega.BeFalse())
	g.Expect(fileExists(itr3.(*Reader).path)).To(gomega.BeFalse())

	// Finalizer.
	itr4path := ""
	if list.Len() > 0 {
		itr4 := list.Iter()
		itr4path = itr4.(*Reader).path
	}
	runtime.GC()
	g.Expect(fileExists(itr4path)).To(gomega.BeFalse())

	// List closed.
	list.Close()
	g.Expect(fileExists(list.writer.path)).To(gomega.BeFalse())
}
