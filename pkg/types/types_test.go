package types

import "testing"

func TestSnapshotCopy(t *testing.T) {
	s := &Snapshot{
		Id: "123",
		Metadata: map[string]string{
			"foo": "bar",
		},
	}

	copied := s.DeepCopy()

	s.Id = "555"
	s.Metadata["foo"] = "foobar"

	if copied.Metadata["foo"] != "bar" {
		t.Errorf("snapshot deepcopy failed")
	}
}
