package resp3

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"
	. "testing"

	"errors"

	"github.com/mediocregopher/radix/v3/resp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeekAndAssertPrefix(t *T) {
	type test struct {
		in     []byte
		prefix Prefix
		exp    error
	}

	tests := []test{
		{[]byte(":5\r\n"), NumberPrefix, nil},
		{[]byte(":5\r\n"), SimpleStringPrefix, resp.ErrConnUsable{
			Err: errUnexpectedPrefix{
				Prefix: NumberPrefix, ExpectedPrefix: SimpleStringPrefix,
			},
		}},
		{[]byte("-foo\r\n"), SimpleErrorPrefix, nil},
		// TODO BlobErrorPrefix
		{[]byte("-foo\r\n"), NumberPrefix, resp.ErrConnUsable{Err: SimpleError{
			S: "foo",
		}}},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			br := bufio.NewReader(bytes.NewReader(test.in))
			err := peekAndAssertPrefix(br, test.prefix, false)

			assert.IsType(t, test.exp, err)
			if expUsable, ok := test.exp.(resp.ErrConnUsable); ok {
				usable, _ := err.(resp.ErrConnUsable)
				assert.IsType(t, expUsable.Err, usable.Err)
			}
			if test.exp != nil {
				assert.Equal(t, test.exp.Error(), err.Error())
			}
		})
	}
}

func TestRESPTypes(t *T) {
	// TODO only used by BulkReader test
	//newLR := func(s string) resp.LenReader {
	//	buf := bytes.NewBufferString(s)
	//	return resp.NewLenReader(buf, int64(buf.Len()))
	//}

	newBigInt := func(s string) *big.Int {
		i, _ := new(big.Int).SetString(s, 10)
		return i
	}

	type encodeTest struct {
		in  resp.Marshaler
		exp string

		// unmarshal is the string to unmarshal. defaults to exp if not set.
		unmarshal string

		// if set then when exp is unmarshaled back into in the value will be
		// asserted to be this value rather than in.
		expUnmarshal interface{}

		errStr bool

		// if set then the test won't be performed a second time with a
		// preceding attribute element when unmarshaling
		noAttrTest bool
	}

	encodeTests := []encodeTest{
		{in: &BlobStringBytes{B: nil}, exp: "$0\r\n\r\n"},
		{in: &BlobStringBytes{B: []byte{}}, exp: "$0\r\n\r\n",
			expUnmarshal: &BlobStringBytes{B: nil}},
		{in: &BlobStringBytes{B: []byte("foo")}, exp: "$3\r\nfoo\r\n"},
		{in: &BlobStringBytes{B: []byte("foo\r\nbar")}, exp: "$8\r\nfoo\r\nbar\r\n"},
		{in: &BlobStringBytes{StreamedStringHeader: true}, exp: "$?\r\n"},
		{in: &BlobString{S: ""}, exp: "$0\r\n\r\n"},
		{in: &BlobString{S: "foo"}, exp: "$3\r\nfoo\r\n"},
		{in: &BlobString{S: "foo\r\nbar"}, exp: "$8\r\nfoo\r\nbar\r\n"},
		{in: &BlobString{StreamedStringHeader: true}, exp: "$?\r\n"},

		{in: &SimpleString{S: ""}, exp: "+\r\n"},
		{in: &SimpleString{S: "foo"}, exp: "+foo\r\n"},

		{in: &SimpleError{S: ""}, exp: "-\r\n", errStr: true},
		{in: &SimpleError{S: "foo"}, exp: "-foo\r\n", errStr: true},

		{in: &Number{N: 5}, exp: ":5\r\n"},
		{in: &Number{N: 0}, exp: ":0\r\n"},
		{in: &Number{N: -5}, exp: ":-5\r\n"},

		{in: &Null{}, exp: "_\r\n"},
		{in: &Null{}, exp: "_\r\n", unmarshal: "$-1\r\n"},
		{in: &Null{}, exp: "_\r\n", unmarshal: "*-1\r\n"},

		{in: &Double{F: 0}, exp: ",0\r\n"},
		{in: &Double{F: 1.5}, exp: ",1.5\r\n"},
		{in: &Double{F: -1.5}, exp: ",-1.5\r\n"},
		{in: &Double{F: math.Inf(1)}, exp: ",inf\r\n"},
		{in: &Double{F: math.Inf(-1)}, exp: ",-inf\r\n"},

		{in: &Boolean{B: false}, exp: "#f\r\n"},
		{in: &Boolean{B: true}, exp: "#t\r\n"},

		{in: &BlobError{B: []byte("")}, exp: "!0\r\n\r\n"},
		{in: &BlobError{B: []byte("foo")}, exp: "!3\r\nfoo\r\n"},
		{in: &BlobError{B: []byte("foo\r\nbar")}, exp: "!8\r\nfoo\r\nbar\r\n"},

		{in: &VerbatimStringBytes{B: nil, Format: []byte("txt")}, exp: "=4\r\ntxt:\r\n"},
		{in: &VerbatimStringBytes{B: []byte{}, Format: []byte("txt")}, exp: "=4\r\ntxt:\r\n",
			expUnmarshal: &VerbatimStringBytes{B: nil, Format: []byte("txt")}},
		{in: &VerbatimStringBytes{B: []byte("foo"), Format: []byte("txt")}, exp: "=7\r\ntxt:foo\r\n"},
		{in: &VerbatimStringBytes{B: []byte("foo\r\nbar"), Format: []byte("txt")}, exp: "=12\r\ntxt:foo\r\nbar\r\n"},
		{in: &VerbatimString{S: "", Format: "txt"}, exp: "=4\r\ntxt:\r\n"},
		{in: &VerbatimString{S: "foo", Format: "txt"}, exp: "=7\r\ntxt:foo\r\n"},
		{in: &VerbatimString{S: "foo\r\nbar", Format: "txt"}, exp: "=12\r\ntxt:foo\r\nbar\r\n"},

		{in: &BigNumber{I: newBigInt("3492890328409238509324850943850943825024385")}, exp: "(3492890328409238509324850943850943825024385\r\n"},
		{in: &BigNumber{I: newBigInt("0")}, exp: "(0\r\n"},
		{in: &BigNumber{I: newBigInt("-3492890328409238509324850943850943825024385")}, exp: "(-3492890328409238509324850943850943825024385\r\n"},

		// TODO what to do with BulkReader
		//{in: &BulkReader{LR: newLR("foo\r\nbar")}, exp: "$8\r\nfoo\r\nbar\r\n"},

		{in: &ArrayHeader{NumElems: 0}, exp: "*0\r\n"},
		{in: &ArrayHeader{NumElems: 5}, exp: "*5\r\n"},
		{in: &ArrayHeader{StreamedArrayHeader: true}, exp: "*?\r\n"},

		{in: &MapHeader{NumPairs: 0}, exp: "%0\r\n"},
		{in: &MapHeader{NumPairs: 5}, exp: "%5\r\n"},
		{in: &MapHeader{StreamedMapHeader: true}, exp: "%?\r\n"},

		{in: &SetHeader{NumElems: 0}, exp: "~0\r\n"},
		{in: &SetHeader{NumElems: 5}, exp: "~5\r\n"},
		{in: &SetHeader{StreamedSetHeader: true}, exp: "~?\r\n"},

		{in: &AttributeHeader{NumPairs: 0}, exp: "|0\r\n", noAttrTest: true},
		{in: &AttributeHeader{NumPairs: 5}, exp: "|5\r\n", noAttrTest: true},

		{in: &PushHeader{NumElems: 0}, exp: ">0\r\n"},
		{in: &PushHeader{NumElems: 5}, exp: ">5\r\n"},

		{in: &StreamedStringChunkBytes{B: nil}, exp: ";0\r\n"},
		{in: &StreamedStringChunkBytes{B: []byte{}}, exp: ";0\r\n",
			expUnmarshal: &StreamedStringChunkBytes{B: nil}},
		{in: &StreamedStringChunkBytes{B: []byte("foo")}, exp: ";3\r\nfoo\r\n"},
		{in: &StreamedStringChunkBytes{B: []byte("foo\r\nbar")}, exp: ";8\r\nfoo\r\nbar\r\n"},
		{in: &StreamedStringChunk{S: ""}, exp: ";0\r\n"},
		{in: &StreamedStringChunk{S: "foo"}, exp: ";3\r\nfoo\r\n"},
		{in: &StreamedStringChunk{S: "foo\r\nbar"}, exp: ";8\r\nfoo\r\nbar\r\n"},
	}

	for i, et := range encodeTests {
		typName := reflect.TypeOf(et.in).Elem().String()
		t.Run(fmt.Sprintf("noAttr/%s/%d", typName, i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			err := et.in.MarshalRESP(buf)
			assert.Nil(t, err)
			assert.Equal(t, et.exp, buf.String())

			if et.unmarshal != "" {
				buf.Reset()
				buf.WriteString(et.unmarshal)
			}

			br := bufio.NewReader(buf)
			umr := reflect.New(reflect.TypeOf(et.in).Elem())
			um := umr.Interface().(resp.Unmarshaler)

			err = um.UnmarshalRESP(br)
			assert.Nil(t, err)
			assert.Empty(t, buf.String())

			var exp interface{} = et.in
			var got interface{} = umr.Interface()
			if et.errStr {
				exp = exp.(error).Error()
				got = got.(error).Error()
			} else if et.expUnmarshal != nil {
				exp = et.expUnmarshal
			}
			assert.Equal(t, exp, got)
		})
	}

	// do the unmarshal half of the tests again, but this time with a preceding
	// attribute which should be ignored.
	for i, et := range encodeTests {
		if et.noAttrTest {
			continue
		}

		typName := reflect.TypeOf(et.in).Elem().String()
		t.Run(fmt.Sprintf("attr/%s/%d", typName, i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			buf.WriteString("|1\r\n+foo\r\n+bar\r\n")
			if et.unmarshal != "" {
				buf.WriteString(et.unmarshal)
			} else {
				buf.WriteString(et.exp)
			}

			br := bufio.NewReader(buf)
			umr := reflect.New(reflect.TypeOf(et.in).Elem())
			um := umr.Interface().(resp.Unmarshaler)

			err := um.UnmarshalRESP(br)
			assert.Nil(t, err)
			assert.Empty(t, buf.String())

			var exp interface{} = et.in
			var got interface{} = umr.Interface()
			if et.errStr {
				exp = exp.(error).Error()
				got = got.(error).Error()
			} else if et.expUnmarshal != nil {
				exp = et.expUnmarshal
			}
			assert.Equal(t, exp, got)
		})
	}
}

// structs used for tests
type TestStructInner struct {
	Foo int
	bar int
	Baz string `redis:"BAZ"`
	Buz string `redis:"-"`
	Boz *int64
}

func intPtr(i int) *int {
	return &i
}

type testStructA struct {
	TestStructInner
	Biz []byte
}

type testStructB struct {
	*TestStructInner
	Biz []byte
}

type testStructC struct {
	Biz *string
}

type textCPMarshaler []byte

func (cm textCPMarshaler) MarshalText() ([]byte, error) {
	cm = append(cm, '_')
	return cm, nil
}

type binCPMarshaler []byte

func (cm binCPMarshaler) MarshalBinary() ([]byte, error) {
	cm = append(cm, '_')
	return cm, nil
}

/*
func TestAnyMarshal(t *T) {
	type encodeTest struct {
		in               interface{}
		out              string
		forceStr, flat   bool
		expErr           bool
		expErrConnUsable bool
	}

	var encodeTests = []encodeTest{
		// Bulk strings
		{in: []byte("ohey"), out: "$4\r\nohey\r\n"},
		{in: "ohey", out: "$4\r\nohey\r\n"},
		{in: "", out: "$0\r\n\r\n"},
		{in: true, out: "$1\r\n1\r\n"},
		{in: false, out: "$1\r\n0\r\n"},
		{in: nil, out: "$-1\r\n"},
		{in: nil, forceStr: true, out: "$0\r\n\r\n"},
		{in: []byte(nil), out: "$-1\r\n"},
		{in: []byte(nil), forceStr: true, out: "$0\r\n\r\n"},
		{in: float32(5.5), out: "$3\r\n5.5\r\n"},
		{in: float64(5.5), out: "$3\r\n5.5\r\n"},
		{in: textCPMarshaler("ohey"), out: "$5\r\nohey_\r\n"},
		{in: binCPMarshaler("ohey"), out: "$5\r\nohey_\r\n"},
		{in: "ohey", flat: true, out: "$4\r\nohey\r\n"},

		// Number
		{in: 5, out: ":5\r\n"},
		{in: int64(5), out: ":5\r\n"},
		{in: uint64(5), out: ":5\r\n"},
		{in: int64(5), forceStr: true, out: "$1\r\n5\r\n"},
		{in: uint64(5), forceStr: true, out: "$1\r\n5\r\n"},

		// Error
		{in: errors.New(":("), out: "-:(\r\n"},
		{in: errors.New(":("), forceStr: true, out: "$2\r\n:(\r\n"},

		// Simple arrays
		{in: []string(nil), out: "*-1\r\n"},
		{in: []string(nil), flat: true, out: ""},
		{in: []string{}, out: "*0\r\n"},
		{in: []string{}, flat: true, out: ""},
		{in: []string{"a", "b"}, out: "*2\r\n$1\r\na\r\n$1\r\nb\r\n"},
		{in: []int{1, 2}, out: "*2\r\n:1\r\n:2\r\n"},
		{in: []int{1, 2}, flat: true, out: ":1\r\n:2\r\n"},
		{in: []int{1, 2}, forceStr: true, out: "*2\r\n$1\r\n1\r\n$1\r\n2\r\n"},
		{in: []int{1, 2}, flat: true, forceStr: true, out: "$1\r\n1\r\n$1\r\n2\r\n"},

		// Complex arrays
		{in: []interface{}{}, out: "*0\r\n"},
		{in: []interface{}{"a", 1}, out: "*2\r\n$1\r\na\r\n:1\r\n"},
		{
			in:       []interface{}{"a", 1},
			forceStr: true,
			out:      "*2\r\n$1\r\na\r\n$1\r\n1\r\n",
		},
		{
			in:       []interface{}{"a", 1},
			forceStr: true,
			flat:     true,
			out:      "$1\r\na\r\n$1\r\n1\r\n",
		},
		{
			in:     []interface{}{func() {}},
			expErr: true,
		},
		{
			in:               []interface{}{func() {}},
			flat:             true,
			expErr:           true,
			expErrConnUsable: true,
		},
		{
			in:     []interface{}{"a", func() {}},
			flat:   true,
			expErr: true,
		},

		// Embedded arrays
		{
			in:  []interface{}{[]string{"a", "b"}, []int{1, 2}},
			out: "*2\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n*2\r\n:1\r\n:2\r\n",
		},
		{
			in:   []interface{}{[]string{"a", "b"}, []int{1, 2}},
			flat: true,
			out:  "$1\r\na\r\n$1\r\nb\r\n:1\r\n:2\r\n",
		},
		{
			in: []interface{}{
				[]interface{}{"a"},
				[]interface{}{"b", func() {}},
			},
			expErr: true,
		},
		{
			in: []interface{}{
				[]interface{}{"a"},
				[]interface{}{"b", func() {}},
			},
			flat:   true,
			expErr: true,
		},
		{
			in: []interface{}{
				[]interface{}{func() {}, "a"},
				[]interface{}{"b", func() {}},
			},
			flat:             true,
			expErr:           true,
			expErrConnUsable: true,
		},

		// Maps
		{in: map[string]int(nil), out: "*-1\r\n"},
		{in: map[string]int(nil), flat: true, out: ""},
		{in: map[string]int{}, out: "*0\r\n"},
		{in: map[string]int{}, flat: true, out: ""},
		{in: map[string]int{"one": 1}, out: "*2\r\n$3\r\none\r\n:1\r\n"},
		{
			in:  map[string]interface{}{"one": []byte("1")},
			out: "*2\r\n$3\r\none\r\n$1\r\n1\r\n",
		},
		{
			in:  map[string]interface{}{"one": []string{"1", "2"}},
			out: "*2\r\n$3\r\none\r\n*2\r\n$1\r\n1\r\n$1\r\n2\r\n",
		},
		{
			in:   map[string]interface{}{"one": []string{"1", "2"}},
			flat: true,
			out:  "$3\r\none\r\n$1\r\n1\r\n$1\r\n2\r\n",
		},
		{
			in:     map[string]interface{}{"one": func() {}},
			expErr: true,
		},
		{
			in:     map[string]interface{}{"one": func() {}},
			flat:   true,
			expErr: true,
		},
		{
			in:     map[complex128]interface{}{0: func() {}},
			expErr: true,
		},
		{
			in:               map[complex128]interface{}{0: func() {}},
			flat:             true,
			expErr:           true,
			expErrConnUsable: true,
		},

		// Structs
		{
			in: testStructA{
				TestStructInner: TestStructInner{
					Foo: 1,
					bar: 2,
					Baz: "3",
					Buz: "4",
					Boz: intPtr(5),
				},
				Biz: []byte("10"),
			},
			out: "*8\r\n" +
				"$3\r\nFoo\r\n" + ":1\r\n" +
				"$3\r\nBAZ\r\n" + "$1\r\n3\r\n" +
				"$3\r\nBoz\r\n" + ":5\r\n" +
				"$3\r\nBiz\r\n" + "$2\r\n10\r\n",
		},
		{
			in: testStructB{
				TestStructInner: &TestStructInner{
					Foo: 1,
					bar: 2,
					Baz: "3",
					Buz: "4",
					Boz: intPtr(5),
				},
				Biz: []byte("10"),
			},
			out: "*8\r\n" +
				"$3\r\nFoo\r\n" + ":1\r\n" +
				"$3\r\nBAZ\r\n" + "$1\r\n3\r\n" +
				"$3\r\nBoz\r\n" + ":5\r\n" +
				"$3\r\nBiz\r\n" + "$2\r\n10\r\n",
		},
		{
			in:  testStructB{Biz: []byte("10")},
			out: "*2\r\n" + "$3\r\nBiz\r\n" + "$2\r\n10\r\n",
		},
		{
			in:  testStructC{},
			out: "*2\r\n" + "$3\r\nBiz\r\n" + "$0\r\n\r\n",
		},
	}

	for i, et := range encodeTests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			a := Any{
				I:                     et.in,
				MarshalBlobString:     et.forceStr,
				MarshalNoArrayHeaders: et.flat,
			}

			err := a.MarshalRESP(buf)
			var errConnUsable resp.ErrConnUsable
			if et.expErr && err == nil {
				t.Fatal("expected error")
			} else if et.expErr && et.expErrConnUsable != errors.As(err, &errConnUsable) {
				t.Fatalf("expected ErrConnUsable:%v, got: %v", et.expErrConnUsable, err)
			} else if !et.expErr {
				assert.Nil(t, err)
			}

			if !et.expErr {
				assert.Equal(t, et.out, buf.String(), "et: %#v", et)
			}
		})
	}
}
*/

type textCPUnmarshaler []byte

func (cu *textCPUnmarshaler) UnmarshalText(b []byte) error {
	*cu = (*cu)[:0]
	*cu = append(*cu, b...)
	return nil
}

type binCPUnmarshaler []byte

func (cu *binCPUnmarshaler) UnmarshalBinary(b []byte) error {
	*cu = (*cu)[:0]
	*cu = append(*cu, b...)
	return nil
}

type lowerCaseUnmarshaler string

func (lcu *lowerCaseUnmarshaler) UnmarshalRESP(br *bufio.Reader) error {
	var bs BlobString
	if err := bs.UnmarshalRESP(br); err != nil {
		return err
	}
	*lcu = lowerCaseUnmarshaler(strings.ToLower(bs.S))
	return nil
}

type upperCaseUnmarshaler string

func (ucu *upperCaseUnmarshaler) UnmarshalRESP(br *bufio.Reader) error {
	var bs BlobString
	if err := bs.UnmarshalRESP(br); err != nil {
		return err
	}
	*ucu = upperCaseUnmarshaler(strings.ToUpper(bs.S))
	return nil
}

type writer []byte

func (w *writer) Write(b []byte) (int, error) {
	*w = append(*w, b...)
	return len(b), nil
}

func TestAnyUnmarshal(t *T) {

	type ie [2]interface{} // into/expect

	type kase struct { // "case" is reserved
		ie ie
		r  bool // reverseable, also test AnyMarshaler
	}

	type decodeTest struct {
		descr string
		ins   []string

		// all ins will be unmarshaled into a pointer to an empty interface, and
		// that interface will be asserted to be equal to this value.
		defaultOut interface{}

		// for each into/exp combination the in field will be unmarshaled into a
		// pointer to the first element of the ie, then the first and second
		// elements of the ie will be asserted to be equal.
		mkCases func(Prefix) []kase

		// instead of testing cases and defaultOut, assert that unmarshal returns
		// this specific error.
		shouldErr error
	}

	strPtr := func(s string) *string { return &s }
	bytPtr := func(b []byte) *[]byte { return &b }
	intPtr := func(i int64) *int64 { return &i }
	fltPtr := func(f float64) *float64 { return &f }

	setReversable := func(to bool, kk []kase) []kase {
		for i := range kk {
			kk[i].r = to
		}
		return kk
	}

	strCases := func(prefix Prefix, str string) []kase {
		return setReversable(prefix == BlobStringPrefix, []kase{
			{ie: ie{"", str}},
			{ie: ie{"otherstring", str}},
			{ie: ie{(*string)(nil), strPtr(str)}},
			{ie: ie{strPtr(""), strPtr(str)}},
			{ie: ie{strPtr("otherstring"), strPtr(str)}},
			{ie: ie{[]byte{}, []byte(str)}},
			{ie: ie{[]byte(nil), []byte(str)}},
			{ie: ie{[]byte("f"), []byte(str)}},
			{ie: ie{[]byte("biglongstringblaaaaah"), []byte(str)}},
			{ie: ie{(*[]byte)(nil), bytPtr([]byte(str))}},
			{ie: ie{bytPtr(nil), bytPtr([]byte(str))}},
			{ie: ie{bytPtr([]byte("f")), bytPtr([]byte(str))}},
			{ie: ie{bytPtr([]byte("biglongstringblaaaaah")), bytPtr([]byte(str))}},
			{ie: ie{textCPUnmarshaler{}, textCPUnmarshaler(str)}},
			{ie: ie{binCPUnmarshaler{}, binCPUnmarshaler(str)}},
			{ie: ie{writer{}, writer(str)}},
		})
	}

	floatCases := func(prefix Prefix, f float64) []kase {
		return setReversable(prefix == DoublePrefix, []kase{
			{ie: ie{float32(0), float32(f)}},
			{ie: ie{float32(1), float32(f)}},
			{ie: ie{float64(0), float64(f)}},
			{ie: ie{float64(1), float64(f)}},
			{ie: ie{(*float64)(nil), fltPtr(f)}},
			{ie: ie{fltPtr(0), fltPtr(f)}},
			{ie: ie{fltPtr(1), fltPtr(f)}},
			{ie: ie{new(big.Float), new(big.Float).SetFloat64(f)}},
			{ie: ie{false, f != 0}},
		})
	}

	intCases := func(prefix Prefix, i int64) []kase {
		kases := floatCases(prefix, float64(i))
		kases = append(kases, setReversable(prefix == NumberPrefix, []kase{
			{ie: ie{int(0), int(i)}},
			{ie: ie{int8(0), int8(i)}},
			{ie: ie{int16(0), int16(i)}},
			{ie: ie{int32(0), int32(i)}},
			{ie: ie{int64(0), int64(i)}},
			{ie: ie{int(1), int(i)}},
			{ie: ie{int8(1), int8(i)}},
			{ie: ie{int16(1), int16(i)}},
			{ie: ie{int32(1), int32(i)}},
			{ie: ie{int64(1), int64(i)}},
			{ie: ie{(*int64)(nil), intPtr(i)}},
			{ie: ie{intPtr(0), intPtr(i)}},
			{ie: ie{intPtr(1), intPtr(i)}},
		})...)

		kases = append(kases, kase{
			ie: ie{new(big.Int), new(big.Int).SetInt64(i)},
			r:  prefix == BigNumberPrefix,
		})

		if i >= 0 {
			kases = append(kases, setReversable(prefix == NumberPrefix, []kase{
				{ie: ie{uint(0), uint(i)}},
				{ie: ie{uint8(0), uint8(i)}},
				{ie: ie{uint16(0), uint16(i)}},
				{ie: ie{uint32(0), uint32(i)}},
				{ie: ie{uint64(0), uint64(i)}},
				{ie: ie{uint(1), uint(i)}},
				{ie: ie{uint8(1), uint8(i)}},
				{ie: ie{uint16(1), uint16(i)}},
				{ie: ie{uint32(1), uint32(i)}},
				{ie: ie{uint64(1), uint64(i)}},
			})...)
		}
		return kases
	}

	nullCases := func(prefix Prefix) []kase {
		return setReversable(prefix == NullPrefix, []kase{
			{ie: ie{[]byte(nil), []byte(nil)}},
			{ie: ie{[]byte{}, []byte(nil)}},
			{ie: ie{[]byte{1}, []byte(nil)}},
			{ie: ie{[]string(nil), []string(nil)}},
			{ie: ie{[]string{}, []string(nil)}},
			{ie: ie{[]string{"ohey"}, []string(nil)}},
			{ie: ie{map[string]string(nil), map[string]string(nil)}},
			{ie: ie{map[string]string{}, map[string]string(nil)}},
			{ie: ie{map[string]string{"a": "b"}, map[string]string(nil)}},
			{ie: ie{(*int64)(nil), (*int64)(nil)}},
			{ie: ie{intPtr(0), (*int64)(nil)}},
			{ie: ie{intPtr(1), (*int64)(nil)}},
		})
	}

	decodeTests := []decodeTest{
		{
			descr:      "empty blob string",
			ins:        []string{"$0\r\n\r\n"},
			defaultOut: []byte{},
			mkCases:    func(prefix Prefix) []kase { return strCases(prefix, "") },
		},
		{
			descr:      "blob string",
			ins:        []string{"$4\r\nohey\r\n"},
			defaultOut: []byte("ohey"),
			mkCases:    func(prefix Prefix) []kase { return strCases(prefix, "ohey") },
		},
		{
			descr:      "integer blob string",
			ins:        []string{"$2\r\n10\r\n"},
			defaultOut: []byte("10"),
			mkCases:    func(prefix Prefix) []kase { return append(strCases(prefix, "10"), intCases(prefix, 10)...) },
		},
		{
			descr:      "float blob string",
			ins:        []string{"$4\r\n10.5\r\n"},
			defaultOut: []byte("10.5"),
			mkCases:    func(prefix Prefix) []kase { return append(strCases(prefix, "10.5"), floatCases(prefix, 10.5)...) },
		},
		{
			descr:      "null blob string", // only for backwards compatibility
			ins:        []string{"$-1\r\n"},
			defaultOut: []byte(nil),
			mkCases:    func(prefix Prefix) []kase { return nullCases(prefix) },
		},
		{
			descr:      "blob string with delim",
			ins:        []string{"$6\r\nab\r\ncd\r\n"},
			defaultOut: []byte("ab\r\ncd"),
			mkCases:    func(prefix Prefix) []kase { return strCases(prefix, "ab\r\ncd") },
		},
		{
			descr:      "empty simple string",
			ins:        []string{"+\r\n"},
			defaultOut: "",
			mkCases:    func(prefix Prefix) []kase { return strCases(prefix, "") },
		},
		{
			descr:      "simple string",
			ins:        []string{"+ohey\r\n"},
			defaultOut: "ohey",
			mkCases:    func(prefix Prefix) []kase { return strCases(prefix, "ohey") },
		},
		{
			descr:      "integer simple string",
			ins:        []string{"+10\r\n"},
			defaultOut: "10",
			mkCases:    func(prefix Prefix) []kase { return append(strCases(prefix, "10"), intCases(prefix, 10)...) },
		},
		{
			descr:      "float simple string",
			ins:        []string{"+10.5\r\n"},
			defaultOut: "10.5",
			mkCases:    func(prefix Prefix) []kase { return append(strCases(prefix, "10.5"), floatCases(prefix, 10.5)...) },
		},
		{
			descr:     "empty simple error",
			ins:       []string{"-\r\n"},
			shouldErr: resp.ErrConnUsable{Err: SimpleError{S: ""}},
		},
		{
			descr:     "simple error",
			ins:       []string{"-ohey\r\n"},
			shouldErr: resp.ErrConnUsable{Err: SimpleError{S: "ohey"}},
		},
		{
			descr:      "zero number",
			ins:        []string{":0\r\n"},
			defaultOut: int64(0),
			mkCases:    func(prefix Prefix) []kase { return append(strCases(prefix, "0"), intCases(prefix, 0)...) },
		},
		{
			descr:      "positive number",
			ins:        []string{":10\r\n"},
			defaultOut: int64(10),
			mkCases:    func(prefix Prefix) []kase { return append(strCases(prefix, "10"), intCases(prefix, 10)...) },
		},
		{
			descr:      "negative number",
			ins:        []string{":-10\r\n"},
			defaultOut: int64(-10),
			mkCases:    func(prefix Prefix) []kase { return append(strCases(prefix, "-10"), intCases(prefix, -10)...) },
		},
		{
			descr:      "null",
			ins:        []string{"_\r\n"},
			defaultOut: nil,
			mkCases:    func(prefix Prefix) []kase { return nullCases(prefix) },
		},
		{
			descr:      "zero double",
			ins:        []string{",0\r\n"},
			defaultOut: float64(0),
			mkCases:    func(prefix Prefix) []kase { return append(strCases(prefix, "0"), floatCases(prefix, 0)...) },
		},
		{
			descr:      "positive double",
			ins:        []string{",10.5\r\n"},
			defaultOut: float64(10),
			mkCases:    func(prefix Prefix) []kase { return append(strCases(prefix, "10.5"), floatCases(prefix, 10.5)...) },
		},
		{
			descr:      "positive double infinity",
			ins:        []string{",inf\r\n"},
			defaultOut: math.Inf(1),
			mkCases:    func(prefix Prefix) []kase { return append(strCases(prefix, "inf"), floatCases(prefix, math.Inf(1))...) },
		},
		{
			descr:      "negative double",
			ins:        []string{",-10.5\r\n"},
			defaultOut: float64(-10),
			mkCases:    func(prefix Prefix) []kase { return append(strCases(prefix, "-10.5"), floatCases(prefix, -10.5)...) },
		},
		{
			descr:      "negative double infinity",
			ins:        []string{",-inf\r\n"},
			defaultOut: math.Inf(-1),
			mkCases: func(prefix Prefix) []kase {
				return append(strCases(prefix, "-inf"), floatCases(prefix, math.Inf(-1))...)
			},
		},
		{
			descr:      "true",
			ins:        []string{"#t\r\n"},
			defaultOut: true,
			// intCases will include actually unmarshaling into a bool
			mkCases: func(prefix Prefix) []kase { return append(strCases(prefix, "1"), intCases(prefix, 1)...) },
		},
		{
			descr:      "false",
			ins:        []string{"#f\r\n"},
			defaultOut: false,
			// intCases will include actually unmarshaling into a bool
			mkCases: func(prefix Prefix) []kase { return append(strCases(prefix, "0"), intCases(prefix, 0)...) },
		},
		{
			descr:     "empty blob error",
			ins:       []string{"!0\r\n\r\n"},
			shouldErr: resp.ErrConnUsable{Err: BlobError{B: []byte{}}},
		},
		{
			descr:     "blob error",
			ins:       []string{"!4\r\nohey\r\n"},
			shouldErr: resp.ErrConnUsable{Err: BlobError{B: []byte("ohey")}},
		},
		{
			descr:     "blob error with delim",
			ins:       []string{"!6\r\noh\r\ney\r\n"},
			shouldErr: resp.ErrConnUsable{Err: BlobError{B: []byte("oh\r\ney")}},
		},
		{
			descr:      "empty verbatim string",
			ins:        []string{"=4\r\ntxt:\r\n"},
			defaultOut: "",
			mkCases:    func(prefix Prefix) []kase { return strCases(prefix, "") },
		},
		{
			descr:      "verbatim string",
			ins:        []string{"=8\r\ntxt:ohey\r\n"},
			defaultOut: "",
			mkCases:    func(prefix Prefix) []kase { return strCases(prefix, "ohey") },
		},
		{
			descr:      "verbatim string with delim",
			ins:        []string{"=10\r\ntxt:oh\r\ney\r\n"},
			defaultOut: "",
			mkCases:    func(prefix Prefix) []kase { return strCases(prefix, "oh\r\ney") },
		},
		{
			descr:      "zero big number",
			ins:        []string{"(0\r\n"},
			defaultOut: new(big.Int),
			mkCases:    func(prefix Prefix) []kase { return append(strCases(prefix, "0"), intCases(prefix, 0)...) },
		},
		{
			descr:      "positive big number",
			ins:        []string{"(1000\r\n"},
			defaultOut: new(big.Int).SetInt64(1000),
			mkCases:    func(prefix Prefix) []kase { return append(strCases(prefix, "1000"), intCases(prefix, 1000)...) },
		},
		{
			descr:      "negative big number",
			ins:        []string{"(-1000\r\n"},
			defaultOut: new(big.Int).SetInt64(-1000),
			mkCases:    func(prefix Prefix) []kase { return append(strCases(prefix, "-1000"), intCases(prefix, -1000)...) },
		},
		{
			descr:      "null array", // only for backwards compatibility
			ins:        []string{"*-1\r\n"},
			defaultOut: []interface{}(nil),
			mkCases:    func(prefix Prefix) []kase { return nullCases(prefix) },
		},
		{
			descr: "empty agg",
			ins: []string{
				"*0\r\n",
				"%0\r\n",
				"~0\r\n",
				// push cannot be empty, don't test it here

				// equivalent streamed aggs
				"*?\r\n.\r\n",
				"%?\r\n.\r\n",
				"~?\r\n.\r\n",
			},
			defaultOut: []interface{}{},
			mkCases: func(prefix Prefix) []kase {
				return []kase{
					{ie: ie{[][]byte(nil), [][]byte{}}},
					{ie: ie{[][]byte{}, [][]byte{}}},
					{ie: ie{[][]byte{[]byte("a")}, [][]byte{}}},
					{ie: ie{[]string(nil), []string{}}},
					{ie: ie{[]string{}, []string{}}},
					{ie: ie{[]string{"a"}, []string{}}},
					{ie: ie{[]int(nil), []int{}}},
					{ie: ie{[]int{}, []int{}}},
					{ie: ie{[]int{5}, []int{}}},
					{ie: ie{map[string][]byte(nil), map[string][]byte{}}},
					{ie: ie{map[string][]byte{}, map[string][]byte{}}},
					{ie: ie{map[string][]byte{"a": []byte("b")}, map[string][]byte{}}},
					{ie: ie{map[string]string(nil), map[string]string{}}},
					{ie: ie{map[string]string{}, map[string]string{}}},
					{ie: ie{map[string]string{"a": "b"}, map[string]string{}}},
					{ie: ie{map[int]int(nil), map[int]int{}}},
					{ie: ie{map[int]int{}, map[int]int{}}},
					{ie: ie{map[int]int{5: 5}, map[int]int{}}},
				}
			},
		},
		{
			descr: "two element agg",
			ins: []string{
				"*2\r\n+666\r\n:1\r\n",
				"%1\r\n+666\r\n:1\r\n",
				"~2\r\n+666\r\n:1\r\n",
				">2\r\n+666\r\n:1\r\n",

				// equivalent streamed aggs
				"*?\r\n+666\r\n:1\r\n.\r\n",
				"%?\r\n+666\r\n:1\r\n.\r\n",
				"~?\r\n+666\r\n:1\r\n.\r\n",
			},
			defaultOut: []interface{}{"666", 1},
			mkCases: func(prefix Prefix) []kase {
				return []kase{
					{ie: ie{[][]byte(nil), [][]byte{[]byte("666"), []byte("1")}}},
					{ie: ie{[][]byte{}, [][]byte{[]byte("666"), []byte("1")}}},
					{ie: ie{[][]byte{[]byte("a")}, [][]byte{[]byte("666"), []byte("1")}}},
					{ie: ie{[]string(nil), []string{"666", "1"}}},
					{ie: ie{[]string{}, []string{"666", "1"}}},
					{ie: ie{[]string{"a"}, []string{"666", "1"}}},
					{ie: ie{[]int(nil), []int{666, 1}}},
					{ie: ie{[]int{}, []int{666, 1}}},
					{ie: ie{[]int{5}, []int{666, 1}}},
					{ie: ie{map[string][]byte(nil), map[string][]byte{"666": []byte("1")}}},
					{ie: ie{map[string][]byte{}, map[string][]byte{"666": []byte("1")}}},
					{ie: ie{map[string][]byte{"a": []byte("b")}, map[string][]byte{"666": []byte("1")}}},
					{ie: ie{map[string]string(nil), map[string]string{"666": "1"}}},
					{ie: ie{map[string]string{}, map[string]string{"666": "1"}}},
					{ie: ie{map[string]string{"a": "b"}, map[string]string{"666": "1"}}},
					{ie: ie{map[int]int(nil), map[int]int{666: 1}}},
					{ie: ie{map[int]int{}, map[int]int{666: 1}}},
					{ie: ie{map[int]int{5: 5}, map[int]int{666: 1}}},
				}
			},
		},
		{
			descr: "nested two element agg",
			ins: []string{
				"*1\r\n*2\r\n+666\r\n:1\r\n",
				"*1\r\n%1\r\n+666\r\n:1\r\n",
				"~1\r\n~2\r\n+666\r\n:1\r\n",
				"*1\r\n>2\r\n+666\r\n:1\r\n", // this is not possible but w/e

				// equivalent streamed aggs
				"*?\r\n*2\r\n+666\r\n:1\r\n.\r\n",
				"*?\r\n%1\r\n+666\r\n:1\r\n.\r\n",
				"~?\r\n~2\r\n+666\r\n:1\r\n.\r\n",
			},
			defaultOut: []interface{}{[]interface{}{"666", 1}},
			mkCases: func(prefix Prefix) []kase {
				return []kase{
					{ie: ie{[][][]byte(nil), [][][]byte{{[]byte("666"), []byte("1")}}}},
					{ie: ie{[][][]byte{}, [][][]byte{{[]byte("666"), []byte("1")}}}},
					{ie: ie{[][][]byte{{}, {[]byte("a")}}, [][][]byte{{[]byte("666"), []byte("1")}}}},
					{ie: ie{[][]string(nil), [][]string{{"666", "1"}}}},
					{ie: ie{[][]string{}, [][]string{{"666", "1"}}}},
					{ie: ie{[][]string{{}, {"a"}}, [][]string{{"666", "1"}}}},
					{ie: ie{[][]int(nil), [][]int{{666, 1}}}},
					{ie: ie{[][]int{}, [][]int{{666, 1}}}},
					{ie: ie{[][]int{{7}, {5}}, [][]int{{666, 1}}}},
					{ie: ie{[]map[string][]byte(nil), []map[string][]byte{{"666": []byte("1")}}}},
					{ie: ie{[]map[string][]byte{}, []map[string][]byte{{"666": []byte("1")}}}},
					{ie: ie{[]map[string][]byte{{}, {"a": []byte("b")}}, []map[string][]byte{{"666": []byte("1")}}}},
					{ie: ie{[]map[string]string(nil), []map[string]string{{"666": "1"}}}},
					{ie: ie{[]map[string]string{}, []map[string]string{{"666": "1"}}}},
					{ie: ie{[]map[string]string{{}, {"a": "b"}}, []map[string]string{{"666": "1"}}}},
					{ie: ie{[]map[int]int(nil), []map[int]int{{666: 1}}}},
					{ie: ie{[]map[int]int{}, []map[int]int{{666: 1}}}},
					{ie: ie{[]map[int]int{{4: 2}, {7: 5}}, []map[int]int{{666: 1}}}},
				}
			},
		},
		{
			descr: "keyed nested two element agg",
			ins: []string{
				"*2\r\n$2\r\n10\r\n*2\r\n+666\r\n:1\r\n",
				"%1\r\n$2\r\n10\r\n%1\r\n+666\r\n:1\r\n",
				"~2\r\n$2\r\n10\r\n~2\r\n+666\r\n:1\r\n",
				">2\r\n$2\r\n10\r\n>2\r\n+666\r\n:1\r\n",

				// equivalent streamed aggs
				"*?\r\n$2\r\n10\r\n*2\r\n+666\r\n:1\r\n.\r\n",
				"%?\r\n$2\r\n10\r\n%1\r\n+666\r\n:1\r\n.\r\n",
				"~?\r\n$2\r\n10\r\n~2\r\n+666\r\n:1\r\n.\r\n",
				">?\r\n$2\r\n10\r\n>2\r\n+666\r\n:1\r\n.\r\n",
			},
			defaultOut: []interface{}{[]byte("10"), []interface{}{"666", 1}},
			mkCases: func(prefix Prefix) []kase {
				return []kase{
					{ie: ie{map[string]map[string][]byte(nil), map[string]map[string][]byte{"10": {"666": []byte("1")}}}},
					{ie: ie{map[string]map[string][]byte{}, map[string]map[string][]byte{"10": {"666": []byte("1")}}}},
					{ie: ie{map[string]map[string][]byte{"foo": {"a": []byte("b")}}, map[string]map[string][]byte{"10": {"666": []byte("1")}}}},
					{ie: ie{map[string]map[string]string(nil), map[string]map[string]string{"10": {"666": "1"}}}},
					{ie: ie{map[string]map[string]string{}, map[string]map[string]string{"10": {"666": "1"}}}},
					{ie: ie{map[string]map[string]string{"foo": {"a": "b"}}, map[string]map[string]string{"10": {"666": "1"}}}},
					{ie: ie{map[string]map[int]int(nil), map[string]map[int]int{"10": {666: 1}}}},
					{ie: ie{map[string]map[int]int{}, map[string]map[int]int{"10": {666: 1}}}},
					{ie: ie{map[string]map[int]int{"foo": {4: 2}}, map[string]map[int]int{"10": {666: 1}}}},
					{ie: ie{map[int]map[int]int(nil), map[int]map[int]int{10: {666: 1}}}},
					{ie: ie{map[int]map[int]int{}, map[int]map[int]int{10: {666: 1}}}},
					{ie: ie{map[int]map[int]int{5: {4: 2}}, map[int]map[int]int{10: {666: 1}}}},
				}
			},
		},
		{
			descr: "agg into structs",
			ins: []string{
				"*10\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n",
				"%5\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n",
				"~10\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n",

				// equivalent streamed aggs
				"*?\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n.\r\n",
				"%?\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n.\r\n",
				"~?\r\n+Foo\r\n:1\r\n+BAZ\r\n:2\r\n+Boz\r\n:3\r\n+Biz\r\n:4\r\n+Other\r\n:5\r\n.\r\n",
			},
			defaultOut: []interface{}{"Foo", 1, "BAZ", 2, "Boz", 3, "Biz", 4},
			mkCases: func(prefix Prefix) []kase {
				return []kase{
					{ie: ie{testStructA{}, testStructA{TestStructInner{Foo: 1, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{ie: ie{&testStructA{}, &testStructA{TestStructInner{Foo: 1, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{ie: ie{testStructA{TestStructInner{bar: 6}, []byte("foo")}, testStructA{TestStructInner{Foo: 1, bar: 6, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{ie: ie{&testStructA{TestStructInner{bar: 6}, []byte("foo")}, &testStructA{TestStructInner{Foo: 1, bar: 6, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{ie: ie{testStructB{}, testStructB{&TestStructInner{Foo: 1, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{ie: ie{&testStructB{}, &testStructB{&TestStructInner{Foo: 1, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{ie: ie{testStructB{&TestStructInner{bar: 6}, []byte("foo")}, testStructB{&TestStructInner{Foo: 1, bar: 6, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
					{ie: ie{&testStructB{&TestStructInner{bar: 6}, []byte("foo")}, &testStructB{&TestStructInner{Foo: 1, bar: 6, Baz: "2", Boz: intPtr(3)}, []byte("4")}}},
				}
			},
		},
		{
			descr:      "empty streamed string",
			ins:        []string{"$?\r\n;0\r\n"},
			defaultOut: writer{},
			mkCases:    func(prefix Prefix) []kase { return strCases(prefix, "") },
		},
		{
			descr: "streamed string",
			ins: []string{
				"$?\r\n;4\r\nohey\r\n;0\r\n",
				"$?\r\n;2\r\noh\r\n;2\r\ney\r\n;0\r\n",
				"$?\r\n;1\r\no\r\n;1\r\nh\r\n;2\r\ney\r\n;0\r\n",
			},
			defaultOut: writer("ohey"),
			mkCases:    func(prefix Prefix) []kase { return strCases(prefix, "ohey") },
		},
	}

	for _, dt := range decodeTests {
		t.Run(dt.descr, func(t *testing.T) {
			for i, in := range dt.ins {
				if dt.shouldErr != nil {
					buf := bytes.NewBufferString(in)
					err := Any{}.UnmarshalRESP(bufio.NewReader(buf))
					assert.Equal(t, dt.shouldErr, err)
					assert.Empty(t, buf.Bytes())
					continue
				}

				t.Run("discard", func(t *testing.T) {
					buf := bytes.NewBufferString(in)
					br := bufio.NewReader(buf)
					err := Any{}.UnmarshalRESP(br)
					assert.NoError(t, err)
					assert.Empty(t, buf.Bytes())
				})

				t.Run(fmt.Sprintf("in%d", i), func(t *testing.T) {
					run := func(withAttr bool) func(t *testing.T) {
						return func(t *testing.T) {
							for j, kase := range dt.mkCases(Prefix(in[0])) {
								t.Run(fmt.Sprintf("case%d", j), func(t *testing.T) {
									t.Logf("%q -> %#v", in, kase.ie[0])
									buf := bytes.NewBufferString(in)
									br := bufio.NewReader(buf)

									// test unmarshaling
									if withAttr {
										AttributeHeader{NumPairs: 2}.MarshalRESP(buf)
										SimpleString{S: "foo"}.MarshalRESP(buf)
										SimpleString{S: "1"}.MarshalRESP(buf)
										SimpleString{S: "bar"}.MarshalRESP(buf)
										SimpleString{S: "2"}.MarshalRESP(buf)
									}

									intoOrigVal := reflect.ValueOf(kase.ie[0])
									intoPtr := reflect.New(intoOrigVal.Type())
									intoPtr.Elem().Set(intoOrigVal)

									err := Any{I: intoPtr.Interface()}.UnmarshalRESP(br)
									assert.NoError(t, err)

									into := intoPtr.Elem().Interface()
									exp := kase.ie[1]
									switch exp := exp.(type) {
									case *big.Int:
										assert.Zero(t, exp.Cmp(into.(*big.Int)))
									case *big.Float:
										assert.Zero(t, exp.Cmp(into.(*big.Float)))
									default:
										assert.Equal(t, exp, into)
									}
									assert.Empty(t, buf.Bytes())

									// test marshaling
									err = (Any{I: exp}).MarshalRESP(buf)
									assert.NoError(t, err)
									assert.Equal(t, in, buf.String())
								})
							}
						}
					}

					t.Run("with attr", run(true))
					t.Run("without attr", run(false))
				})
			}
		})
	}
}

func TestRawMessage(t *T) {
	rmtests := []struct {
		b       string
		isNil   bool
		isEmpty bool
	}{
		{b: "+\r\n"},
		{b: "+foo\r\n"},
		{b: "-\r\n"},
		{b: "-foo\r\n"},
		{b: ":5\r\n"},
		{b: ":0\r\n"},
		{b: ":-5\r\n"},
		{b: "$-1\r\n", isNil: true},
		{b: "$0\r\n\r\n"},
		{b: "$3\r\nfoo\r\n"},
		{b: "$8\r\nfoo\r\nbar\r\n"},
		{b: "*2\r\n:1\r\n:2\r\n"},
		{b: "*-1\r\n", isNil: true},
		{b: "*0\r\n", isEmpty: true},
	}

	// one at a time
	for _, rmt := range rmtests {
		buf := new(bytes.Buffer)
		{
			rm := RawMessage(rmt.b)
			require.Nil(t, rm.MarshalRESP(buf))
			assert.Equal(t, rmt.b, buf.String())
			assert.Equal(t, rmt.isNil, rm.IsNil())
			assert.Equal(t, rmt.isEmpty, rm.IsEmptyArray())
		}
		{
			var rm RawMessage
			require.Nil(t, rm.UnmarshalRESP(bufio.NewReader(buf)))
			assert.Equal(t, rmt.b, string(rm))
		}
	}
}

func TestAnyConsumedOnErr(t *T) {
	type foo struct {
		Foo int
		Bar int
	}

	type test struct {
		in   resp.Marshaler
		into interface{}
	}

	type unknownType string

	tests := []test{
		{Any{I: errors.New("foo")}, new(unknownType)},
		{BlobString{S: "blobStr"}, new(unknownType)},
		{SimpleString{S: "blobStr"}, new(unknownType)},
		{Number{N: 1}, new(unknownType)},
		{Any{I: []string{"one", "2", "three"}}, new([]int)},
		{Any{I: []string{"1", "2", "three", "four"}}, new([]int)},
		{Any{I: []string{"1", "2", "3", "four"}}, new([]int)},
		{Any{I: []string{"1", "2", "three", "four", "five"}}, new(map[int]int)},
		{Any{I: []string{"1", "2", "three", "four", "five", "six"}}, new(map[int]int)},
		{Any{I: []string{"1", "2", "3", "four", "five", "six"}}, new(map[int]int)},
		{Any{I: []interface{}{1, 2, "Bar", "two"}}, new(foo)},
		{Any{I: []string{"Foo", "1", "Bar", "two"}}, new(foo)},
		{Any{I: [][]string{{"one", "two"}, {"three", "four"}}}, new([][]int)},
		{Any{I: [][]string{{"1", "two"}, {"three", "four"}}}, new([][]int)},
		{Any{I: [][]string{{"1", "2"}, {"three", "four"}}}, new([][]int)},
		{Any{I: [][]string{{"1", "2"}, {"3", "four"}}}, new([][]int)},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			buf := new(bytes.Buffer)
			require.Nil(t, test.in.MarshalRESP(buf))
			require.Nil(t, SimpleString{S: "DISCARDED"}.MarshalRESP(buf))
			br := bufio.NewReader(buf)

			err := Any{I: test.into}.UnmarshalRESP(br)
			assert.Error(t, err)
			assert.True(t, errors.As(err, new(resp.ErrConnUsable)))

			var ss SimpleString
			assert.NoError(t, ss.UnmarshalRESP(br))
			assert.Equal(t, "DISCARDED", ss.S)
		})
	}
}

func Example_streamedAggregatedType() {
	buf := new(bytes.Buffer)

	// First write a streamed array to the buffer. The array will have 3 number
	// elements.
	(ArrayHeader{StreamedArrayHeader: true}).MarshalRESP(buf)
	(Number{N: 1}).MarshalRESP(buf)
	(Number{N: 2}).MarshalRESP(buf)
	(Number{N: 3}).MarshalRESP(buf)
	(StreamedAggregatedEnd{}).MarshalRESP(buf)

	// Now create a reader which will read from the buffer, and use it to read
	// the streamed array.
	br := bufio.NewReader(buf)
	var head ArrayHeader
	head.UnmarshalRESP(br)
	if !head.StreamedArrayHeader {
		panic("expected streamed array header")
	}
	fmt.Println("streamed array begun")

	for {
		var el Number
		aggEl := StreamedAggregatedElement{Unmarshaler: &el}
		aggEl.UnmarshalRESP(br)
		if aggEl.End {
			fmt.Println("streamed array ended")
			return
		}
		fmt.Printf("read element with value %d\n", el.N)
	}

	// Output: streamed array begun
	// Output: read element with value 1
	// Output: read element with value 2
	// Output: read element with value 3
	// Output: streamed array ended
}
