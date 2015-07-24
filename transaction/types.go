package transaction

type T struct {
	root string
}

func New() *T {
	return nil
}

type Store interface {
	List() []string
	Tx(id string) *T
	Save(tx *T)
	OpenBlob(tx *T, pbid int) io.WriteSeeker
}
