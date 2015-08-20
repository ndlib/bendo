package items

import ()

// Validate the given item. Returns the total amount checksummed (in bytes),
// a list of issues which will be empty if everything is fine, and an error
// if an error happened during the validation. In particular, err does not
// show validation errors, only if a system error happened while validating.
//
// Things checked (not all are implemented yet):
// * Each blob has the correct checksum
// * Each blob appears in exactly one bundle
// * Every blob is assigned to at least one slot in at least one version
// * Each slot points to an existing (possibly deleted) blob
// * Each bundle is readable and in the correct format
// * There are no extra files in a bundle
// * All required metadata fields are present for each blob
// * All required metadata fields are present for each version
func (s *Store) Validate(id string) (size int64, problems []string, err error) {
	// Validation is funny because we not only validate the item's
	// internal integrity, but also the serialization into bundle files.

	return 0, nil, nil
}

func (item *Item) validateChecksums() (size int64, problems []string, err error) {
	return 0, nil, nil
}
