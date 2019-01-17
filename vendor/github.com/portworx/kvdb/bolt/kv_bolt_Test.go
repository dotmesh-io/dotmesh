package bolt

// go test -v .

import (
	"fmt"
	"os"
	"testing"

	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/test"
	"github.com/stretchr/testify/assert"
)

func TestAll(t *testing.T) {
	// test.Run(New, t, Start, Stop)
	// test.RunWatch(New, t, Start, Stop)
	test.RunWatch(New, t, Start, Stop)
}

func testNoCopy(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("testNoCopy")
	type Test struct {
		A int
		B string
	}
	val := Test{1, "abc"}
	_, err := kv.Put("key1", &val, 0)
	assert.NoError(t, err, "Expected no error on put")
	val.A = 2
	val.B = "def"
	var newVal Test
	_, err = kv.GetVal("key1", &newVal)
	assert.NoError(t, err, "Expected no error on get")
	assert.Equal(t, newVal.A, val.A, "Expected equal values")
	assert.Equal(t, newVal.B, val.B, "Expected equal values")
}

func testGetCopy(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("testGetCopy")
	type Test struct {
		A int
		B string
	}
	val := Test{1, "abc"}
	_, err := kv.Put("key-copy", &val, 0)
	assert.NoError(t, err, "Expected no error on put")
	copySelect := func(val interface{}) interface{} {
		cpy := val.(*Test)
		return &Test{cpy.A, cpy.B}
	}
	_, err = kv.GetWithCopy("key-copy", nil)
	assert.Error(t, err, "Expected an error")
	valIntf, err := kv.GetWithCopy("key-copy", copySelect)
	assert.NoError(t, err, "Expected no error on GetWithCopy")
	newVal, ok := valIntf.(*Test)
	assert.True(t, ok, "Expected correct interface to be returned")
	// Modify the actual value
	val.A = 2
	val.B = "def"

	assert.NoError(t, err, "Expected no error on get")
	assert.NotEqual(t, newVal.A, val.A, "Expected unequal values")
	assert.NotEqual(t, newVal.B, val.B, "Expected unequal values")
}

func testEnumerateWithSelect(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("enumerateWithSelect")
	type EWS struct {
		A int
		B string
	}
	a := &EWS{1, "abc"}
	b := &EWS{2, "def"}
	c := &EWS{3, "abcdef"}
	prefix := "ews"
	kv.Put(prefix+"/"+"key1", a, 0)
	kv.Put(prefix+"/"+"key2", b, 0)
	kv.Put(prefix+"/"+"key3", c, 0)
	enumerateSelect := func(val interface{}) bool {
		v, ok := val.(*EWS)
		if !ok {
			return false
		}
		if len(v.B) != 3 {
			return false
		}
		return true
	}
	copySelect := func(val interface{}) interface{} {
		v, ok := val.(*EWS)
		if !ok {
			return nil
		}
		cpy := *v
		return &cpy
	}
	_, err := kv.EnumerateWithSelect(prefix, nil, nil)
	assert.Error(t, err, "Expected error on EnumerateWithSelect")
	output, err := kv.EnumerateWithSelect(prefix, enumerateSelect, copySelect)
	assert.NoError(t, err, "Unexpected error on EnumerateWithSelect")
	assert.Equal(t, 2, len(output), "Unexpected no. of values returned")
	// Check if copy is returned
	for _, out := range output {
		ews, ok := out.(*EWS)
		assert.Equal(t, ok, true, "Unexpected type of object returned")
		if ews.A == a.A {
			a.A = 999
			assert.NotEqual(t, a.A, ews.A, "Copy was not returned")
		} else if ews.A == b.A {
			b.A = 888
			assert.NotEqual(t, b.A, ews.A, "Copy was not returned")
		} else {
			assert.Fail(t, "Unexpected objects returned")
		}
	}
	// Invalid copy function
	invalidCopySelect := func(val interface{}) interface{} {
		return nil
	}
	output, err = kv.EnumerateWithSelect(prefix, enumerateSelect, invalidCopySelect)
	assert.EqualError(t, err, ErrIllegalSelect.Error(), "Unexpected error")
	assert.Equal(t, 0, len(output), "Unexpected output")
}

func Start(removeDir bool) error {
	if removeDir {
		os.RemoveAll("px.db")
	}
	return nil
}

func Stop() error {
	return nil
}
