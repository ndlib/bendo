package fragment

import (
	"testing"

	"github.com/ndlib/bendo/store"
)

type JTest struct {
	Name string
	Age  int
}

func TestJSON(t *testing.T) {
	memory := store.NewMemory()
	js := NewJSON(memory)

	first := JTest{Name: "Petra", Age: 30}
	err := js.Save("petra", &first)
	if err != nil {
		t.Fatalf("Got err = %s, expected nil", err.Error())
	}
	second := new(JTest)
	err = js.Open("petra", &second)
	if err != nil {
		t.Fatalf("Got err = %s, expected nil", err.Error())
	}
	if second.Name != "Petra" || second.Age != 30 {
		t.Fatalf("Got %#v, expected %#v", second, first)
	}
	// directly inject some JSON and see if it is read correctly
	thirdString := `{"Name":"John","Age":50}`
	w, _ := memory.Create("john-2")
	w.Write([]byte(thirdString))
	w.Close()
	third := new(JTest)
	err = js.Open("john-2", &third)
	if err != nil {
		t.Fatalf("Got err = %s, expected nil", err.Error())
	}
	if third.Name != "John" || third.Age != 50 {
		t.Fatalf("Got %#v, expected %#v", third, JTest{"John", 50})
	}
}
