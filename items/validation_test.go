package items

import (
	"testing"

	"github.com/ndlib/bendo/store"
)

func TestValidate(t *testing.T) {
	// Testing validation is difficult since the data needs to be in bundles.
	// We spread a our items over two bundles for testing.

	// make good item
	ms := store.NewMemory()
	s := New(ms)
	err := createBundledItem(t, s, "gooditem", []itemData{
		{bundle: 1,
			slot: "stuff/hello",
			data: "hello"},
		{bundle: 1,
			slot: "stuff/hello2",
			data: "hello2"},
		{bundle: 2,
			slot: "morestuff/hello",
			data: "hello"},
		{bundle: 2,
			slot: "stuff/hello3",
			data: "hello3"},
	})

	if err != nil {
		t.Fatalf("Received %s, expected nil", err.Error())
	}

	nb, problems, err := s.Validate("gooditem")

	t.Logf("nb = %d", nb)
	if len(problems) > 0 {
		t.Errorf("Received problems %v", problems)
	}
	if err != nil {
		t.Errorf("Received error %s", err.Error())
	}

	// making bad items requires mucking with the bundle innards.
	// TODO(dbrower): add tests for bad items.
}

type itemData struct {
	bundle int
	slot   string
	data   string
}

func createBundledItem(t *testing.T, s *Store, name string, data []itemData) error {
	var prevBundle = 1

	w, err := s.Open(name, "nobody")
	if err != nil {
		return err
	}
	for _, d := range data {
		if d.bundle != prevBundle {
			prevBundle = d.bundle
			err := w.Close()
			if err != nil {
				return err
			}
			w, err = s.Open(name, "nobody")
			if err != nil {
				return err
			}
		}
		bid := writedata(t, w, d.data)
		w.SetSlot(d.slot, bid)
	}
	return w.Close()
}
