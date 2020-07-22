// Package resp3 implements the upgraded redis RESP3 protocol, a plaintext
// protocol which is also binary safe and backwards compatible with the original
// RESP2 protocol. Redis uses the RESP protocol to communicate with its clients,
// but there's nothing about the protocol which ties it to redis, it could be
// used for almost anything.
//
// See https://github.com/antirez/RESP3 for more details on the protocol.
package resp3

import (
	"bufio"
	"bytes"
	"encoding"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"sync"

	"errors"

	"github.com/mediocregopher/radix/v3/internal/bytesutil"
	"github.com/mediocregopher/radix/v3/resp"
)

var delim = []byte{'\r', '\n'}

// Prefix enumerates the possible RESP3 types by enumerating the different
// prefixes a RESP3 message might start with.
//
// In order to determine the type of a message which is being written to a
// *bufio.Reader, without actually consuming it, one can use the Peek method and
// compare it against a Prefix value.
type Prefix byte

// Enumeration of each of RESP3 prefices.
var (
	BlobStringPrefix     Prefix = '$'
	SimpleStringPrefix   Prefix = '+'
	SimpleErrorPrefix    Prefix = '-'
	NumberPrefix         Prefix = ':'
	NullPrefix           Prefix = '_'
	DoublePrefix         Prefix = ','
	BooleanPrefix        Prefix = '#'
	BlobErrorPrefix      Prefix = '!'
	VerbatimStringPrefix Prefix = '='
	BigNumberPrefix      Prefix = '('

	ArrayPrefix     Prefix = '*'
	MapPrefix       Prefix = '%'
	SetPrefix       Prefix = '~'
	AttributePrefix Prefix = '|'
	PushPrefix      Prefix = '>'

	StreamedAggregatedTypeEndPrefix Prefix = '.'
	StreamedStringChunkPrefix       Prefix = ';'
)

// String formats a prefix into a human-readable name for the type it denotes.
func (p Prefix) String() string {
	pStr := string(p)
	switch pStr {
	case string(BlobStringPrefix):
		return "blob-string"
	case string(SimpleStringPrefix):
		return "simple-string"
	case string(SimpleErrorPrefix):
		return "simple-error"
	case string(NumberPrefix):
		return "number"
	case string(NullPrefix):
		return "null"
	case string(DoublePrefix):
		return "double"
	case string(BooleanPrefix):
		return "boolean"
	case string(BlobErrorPrefix):
		return "blob-error"
	case string(VerbatimStringPrefix):
		return "verbatim-string"
	case string(BigNumberPrefix):
		return "big-number"
	case string(ArrayPrefix):
		return "array"
	case string(MapPrefix):
		return "map"
	case string(SetPrefix):
		return "set"
	case string(AttributePrefix):
		return "attribute"
	case string(PushPrefix):
		return "push"
	case string(StreamedAggregatedTypeEndPrefix):
		return "streamed-aggregated-type-end"
	case string(StreamedStringChunkPrefix):
		return "streamed-string-chunk"
	default:
		return pStr
	}
}

func (p Prefix) doesPrefix(b []byte) bool {
	if len(b) == 0 {
		panic("empty byte slice should not be passed here, please submit a bug report")
	}

	return Prefix(b[0]) == p
}

var (
	// TODO null values shouldn't be needed here
	nullRESP2Bulktring = []byte("$-1\r\n")
	nullRESP2Array     = []byte("*-1\r\n")
	null               = []byte("_\r\n")
	booleanTrue        = []byte("#t\r\n")
	booleanFalse       = []byte("#f\r\n")
	emptyArray         = []byte("*0\r\n")
	streamHeaderSize   = []byte("?")
	streamAggEnd       = []byte(".\r\n")
)

var boolStrs = [][]byte{
	{'0'},
	{'1'},
}

////////////////////////////////////////////////////////////////////////////////

// l may be negative to indicate that elements should be discarded until a
// streamed aggregated end type message is encountered.
func discardMulti(br *bufio.Reader, l int) error {
	for i := 0; i < l || l < 0; i++ {
		if more, err := (Any{}).maybeUnmarshalRESP(br, l < 0); err != nil {
			return err
		} else if !more {
			return nil
		}
	}
	return nil
}

func discardAttribute(br *bufio.Reader) error {
	var attrHead AttributeHeader
	b, err := br.Peek(1)
	if err != nil {
		return err
	} else if !AttributePrefix.doesPrefix(b) {
		return nil
	} else if err := attrHead.UnmarshalRESP(br); err != nil {
		return nil
	}

	return discardMulti(br, attrHead.NumPairs*2)
}

type errUnexpectedPrefix struct {
	Prefix         Prefix
	ExpectedPrefix Prefix
}

func (e errUnexpectedPrefix) Error() string {
	return fmt.Sprintf("expected prefix %q, got %q", e.ExpectedPrefix, e.Prefix)
}

// peekAndAssertPrefix will peek at the next incoming redis message and assert
// that it is of the type identified by the given RESP prefix (e.g.
// StringStringPrefix, PushPrefix, etc).
//
// If the message is a RESP error (and that wasn't the intended prefix) then it
// will be unmarshaled into the appropriate error type and returned. If the
// message is of any other type (that isn't the intended prefix) it will be
// discarded and errUnexpectedPrefix will be returned.
//
// peekAndAssertPrefix will discard any preceding attribute message when called
// with discardAttr set
func peekAndAssertPrefix(br *bufio.Reader, expectedPrefix Prefix, discardAttr bool) error {
	if discardAttr {
		if err := discardAttribute(br); err != nil {
			return err
		}
	}

	b, err := br.Peek(1)
	if err != nil {
		return err
	} else if expectedPrefix.doesPrefix(b) {
		return nil
	} else if SimpleErrorPrefix.doesPrefix(b) {
		var respErr SimpleError
		if err := respErr.UnmarshalRESP(br); err != nil {
			return err
		}
		return resp.ErrConnUsable{Err: respErr}
	} else if BlobErrorPrefix.doesPrefix(b) {
		panic("TODO")
	} else if err := (Any{}).UnmarshalRESP(br); err != nil {
		return err
	}
	return resp.ErrConnUsable{Err: errUnexpectedPrefix{
		Prefix:         Prefix(b[0]),
		ExpectedPrefix: expectedPrefix,
	}}
}

// like peekAndAssertPrefix, but will consume the prefix if it is the correct
// one as well.
func readAndAssertPrefix(br *bufio.Reader, prefix Prefix, discardAttr bool) error {
	if err := peekAndAssertPrefix(br, prefix, discardAttr); err != nil {
		return err
	}
	_, err := br.Discard(1)
	return err
}

////////////////////////////////////////////////////////////////////////////////

// BlobStringBytes represents the blob string type in the RESP protocol using a
// go byte slice. A B value of nil is an empty string.
//
// BlobStringBytes can also be used as the header message of a streamed string.
// When used in that way it will be followed by one or more BlobStringChunk
// messages, ending in a BlobStringChunk with a zero length.
type BlobStringBytes struct {
	B []byte

	// StreamedStringHeader indicates that this message is the header message of
	// a streamed string. It is mutually exclusive with B.
	StreamedStringHeader bool
}

// MarshalRESP implements the Marshaler method.
func (b BlobStringBytes) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, byte(BlobStringPrefix))
	if b.StreamedStringHeader {
		*scratch = append(*scratch, streamHeaderSize...)
	} else {
		*scratch = strconv.AppendInt(*scratch, int64(len(b.B)), 10)
		*scratch = append(*scratch, delim...)
		*scratch = append(*scratch, b.B...)
	}
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (b *BlobStringBytes) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, BlobStringPrefix, true); err != nil {
		return err
	}

	byt, err := bytesutil.ReadBytesDelim(br)
	if err != nil {
		return err
	} else if bytes.Equal(byt, streamHeaderSize) {
		b.StreamedStringHeader = true
		return nil
	}

	n, err := bytesutil.ParseInt(byt)
	if err != nil {
		return err
	} else if n == -1 {
		return errors.New("BlobStringBytes does not support unmarshaling RESP2 null bulk string")
	} else if n < 0 {
		return fmt.Errorf("invalid blob string length: %d", n)
	} else if n == 0 {
		b.B = nil
	} else {
		b.B = bytesutil.Expand(b.B, int(n), false)
		if _, err := io.ReadFull(br, b.B); err != nil {
			return err
		}
	}

	if _, err := bytesutil.ReadBytesDelim(br); err != nil {
		return err
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// BlobString represents the blob string type in the RESP protocol using a go
// string.
//
// BlobString can also be used as the header message of a streamed string. When
// used in that way it will be followed by one or more BlobStringChunk messages,
// ending in a BlobStringChunk with a zero length.
type BlobString struct {
	S string

	// StreamedStringHeader indicates that this message is the header message of
	// a streamed string. It is mutually exclusive with S.
	StreamedStringHeader bool
}

// MarshalRESP implements the Marshaler method.
func (b BlobString) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, byte(BlobStringPrefix))
	if b.StreamedStringHeader {
		*scratch = append(*scratch, streamHeaderSize...)
	} else {
		*scratch = strconv.AppendInt(*scratch, int64(len(b.S)), 10)
		*scratch = append(*scratch, delim...)
		*scratch = append(*scratch, b.S...)
	}
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (b *BlobString) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, BlobStringPrefix, true); err != nil {
		return err
	}

	byt, err := bytesutil.ReadBytesDelim(br)
	if err != nil {
		return err
	} else if bytes.Equal(byt, streamHeaderSize) {
		b.StreamedStringHeader = true
		return nil
	}

	n, err := bytesutil.ParseInt(byt)
	if err != nil {
		return err
	} else if n == -1 {
		return errors.New("BlobString does not support unmarshaling RESP2 null bulk string")
	} else if n < 0 {
		return fmt.Errorf("invalid blob string length: %d", n)
	} else if n == 0 {
		b.S = ""
	} else {
		scratch := bytesutil.GetBytes()
		defer bytesutil.PutBytes(scratch)

		*scratch = bytesutil.Expand(*scratch, int(n), false)
		if _, err := io.ReadFull(br, *scratch); err != nil {
			return err
		}
		b.S = string(*scratch)
	}

	if _, err := bytesutil.ReadBytesDelim(br); err != nil {
		return err
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

// SimpleString represents the simple string type in the RESP protocol.
type SimpleString struct {
	S string
}

// MarshalRESP implements the Marshaler method.
func (ss SimpleString) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, byte(SimpleStringPrefix))
	*scratch = append(*scratch, ss.S...)
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (ss *SimpleString) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, SimpleStringPrefix, true); err != nil {
		return err
	}
	b, err := bytesutil.ReadBytesDelim(br)
	if err != nil {
		return err
	}

	ss.S = string(b)
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// SimpleError represents the simple error type in the RESP protocol. Note that
// this only represents an actual error message being read/written on the
// stream, it is separate from network or parsing errors.
type SimpleError struct {
	S string
}

func (e SimpleError) Error() string {
	return e.S
}

// MarshalRESP implements the Marshaler method.
func (e SimpleError) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, byte(SimpleErrorPrefix))
	*scratch = append(*scratch, e.S...)
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (e *SimpleError) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, SimpleErrorPrefix, true); err != nil {
		return err
	}
	b, err := bytesutil.ReadBytesDelim(br)
	e.S = string(b)
	return err
}

////////////////////////////////////////////////////////////////////////////////

// Number represents the number type in the RESP protocol.
type Number struct {
	N int64
}

// MarshalRESP implements the Marshaler method.
func (n Number) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, byte(NumberPrefix))
	*scratch = strconv.AppendInt(*scratch, n.N, 10)
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (n *Number) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, NumberPrefix, true); err != nil {
		return err
	}
	i, err := bytesutil.ReadIntDelim(br)
	n.N = i
	return err
}

////////////////////////////////////////////////////////////////////////////////

// Null represents the null type in the RESP protocol.
//
// NOTE that this will always marshal to the RESP3 null type, but for
// convenience is also capable of unmarshaling the RESP2 null bulk string and
// null array values.
type Null struct{}

// MarshalRESP implements the Marshaler method.
func (Null) MarshalRESP(w io.Writer) error {
	_, err := w.Write(null)
	return err
}

var resp2NullPrefix = []byte("-1\r")

// UnmarshalRESP implements the Unmarshaler method.
func (*Null) UnmarshalRESP(br *bufio.Reader) error {
	if err := discardAttribute(br); err != nil {
		return err
	}

	b, err := br.Peek(1)
	if err != nil {
		return err
	}
	prefix := Prefix(b[0])

	switch prefix {
	case NullPrefix:
		b, err := bytesutil.ReadBytesDelim(br)
		if err != nil {
			return err
		} else if len(b) != 1 {
			return errors.New("malformed null resp")
		}
		return nil

	case ArrayPrefix, BlobStringPrefix:
		// no matter what size an array or blob string is it _must_ have at
		// least 4 characters on the wire (prefix+size+delim). So only check
		// that.
		b, err := br.Peek(4)
		if err != nil {
			return err
		} else if !bytes.Equal(b[1:], resp2NullPrefix) {
			if err := (Any{}).UnmarshalRESP(br); err != nil {
				return err
			}
			return resp.ErrConnUsable{Err: errors.New("malformed null resp")}
		}

		// actually consume the message, after all this peeking.
		_, err = bytesutil.ReadBytesDelim(br)
		return err

	default:
		if err := (Any{}).UnmarshalRESP(br); err != nil {
			return err
		}
		return resp.ErrConnUsable{Err: errUnexpectedPrefix{
			Prefix:         prefix,
			ExpectedPrefix: NullPrefix,
		}}
	}
}

////////////////////////////////////////////////////////////////////////////////

// Double represents the double type in the RESP protocol.
type Double struct {
	F float64
}

// MarshalRESP implements the Marshaler method.
func (d Double) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	*scratch = append(*scratch, byte(DoublePrefix))

	if math.IsInf(d.F, 1) {
		*scratch = append(*scratch, "inf"...)
	} else if math.IsInf(d.F, -1) {
		*scratch = append(*scratch, "-inf"...)
	} else {
		*scratch = strconv.AppendFloat(*scratch, d.F, 'f', -1, 64)
	}

	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	bytesutil.PutBytes(scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (d *Double) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, DoublePrefix, true); err != nil {
		return err
	}
	b, err := bytesutil.ReadBytesDelim(br)
	if err != nil {
		return err
	} else if d.F, err = strconv.ParseFloat(string(b), 64); err != nil {
		return resp.ErrConnUsable{
			Err: fmt.Errorf("failed to parse double resp %q as float: %w", b, err),
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// Boolean represents the boolean type in the RESP protocol.
type Boolean struct {
	B bool
}

// MarshalRESP implements the Marshaler method.
func (b Boolean) MarshalRESP(w io.Writer) error {
	var err error
	if b.B {
		_, err = w.Write(booleanTrue)
	} else {
		_, err = w.Write(booleanFalse)
	}
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (b *Boolean) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, BooleanPrefix, true); err != nil {
		return err
	}
	byt, err := bytesutil.ReadBytesDelim(br)
	if err != nil {
		return err
	} else if len(byt) != 1 {
		return errors.New("malformed boolean resp")
	} else if byt[0] == 't' {
		b.B = true
	} else if byt[0] == 'f' {
		b.B = false
	} else {
		return errors.New("malformed boolean resp")
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// BlobError represents the blob error type in the RESP protocol.
type BlobError struct {
	B []byte
}

func (e BlobError) Error() string {
	return string(e.B)
}

// MarshalRESP implements the Marshaler method.
func (e BlobError) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, byte(BlobErrorPrefix))
	*scratch = strconv.AppendInt(*scratch, int64(len(e.B)), 10)
	*scratch = append(*scratch, delim...)
	*scratch = append(*scratch, e.B...)
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (e *BlobError) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, BlobErrorPrefix, true); err != nil {
		return err
	}

	n, err := bytesutil.ReadIntDelim(br)
	if err != nil {
		return err
	} else if n < 0 {
		return fmt.Errorf("invalid blob error length: %d", n)
	}

	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = bytesutil.Expand(*scratch, int(n), false)
	if _, err := io.ReadFull(br, *scratch); err != nil {
		return err
	} else if _, err := bytesutil.ReadBytesDelim(br); err != nil {
		return err
	}

	e.B = *scratch
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// VerbatimStringBytes represents the verbatim string type in the RESP protocol
// using a go byte slice. A B value of nil is an empty string.
type VerbatimStringBytes struct {
	B []byte

	// Format is a 3 character string describing the format that the verbatim
	// string is encoded in, e.g. "txt" or "mkd". MarshalRESP will error without
	// writing anything if this is not exactly 3 characters.
	Format []byte
}

// MarshalRESP implements the Marshaler method.
func (b VerbatimStringBytes) MarshalRESP(w io.Writer) error {
	if len(b.Format) != 3 {
		return resp.ErrConnUsable{
			Err: errors.New("Format field must be exactly 3 characters"),
		}
	}
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, byte(VerbatimStringPrefix))
	*scratch = strconv.AppendInt(*scratch, int64(len(b.B))+4, 10)
	*scratch = append(*scratch, delim...)
	*scratch = append(*scratch, b.Format...)
	*scratch = append(*scratch, ':')
	*scratch = append(*scratch, b.B...)
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (b *VerbatimStringBytes) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, VerbatimStringPrefix, true); err != nil {
		return err
	}

	n, err := bytesutil.ReadIntDelim(br)
	if err != nil {
		return err
	} else if n < 4 {
		return fmt.Errorf("invalid verbatim string length: %d", n)
	}

	b.B = bytesutil.Expand(b.B, int(n), false)
	if _, err := io.ReadFull(br, b.B); err != nil {
		return err
	} else if _, err := bytesutil.ReadBytesDelim(br); err != nil {
		return err
	}

	b.Format, b.B = b.B[:3], b.B[4:]
	if len(b.B) == 0 {
		b.B = nil
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

// VerbatimString represents the verbatim string type in the RESP protocol
// using a go string.
type VerbatimString struct {
	S string

	// Format is a 3 character string describing the format that the verbatim
	// string is encoded in, e.g. "txt" or "mkd". MarshalRESP will error without
	// writing anything if this is not exactly 3 characters.
	Format string
}

// MarshalRESP implements the Marshaler method.
func (b VerbatimString) MarshalRESP(w io.Writer) error {
	if len(b.Format) != 3 {
		return resp.ErrConnUsable{
			Err: errors.New("Format field must be exactly 3 characters"),
		}
	}
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, byte(VerbatimStringPrefix))
	*scratch = strconv.AppendInt(*scratch, int64(len(b.S))+4, 10)
	*scratch = append(*scratch, delim...)
	*scratch = append(*scratch, b.Format...)
	*scratch = append(*scratch, ':')
	*scratch = append(*scratch, b.S...)
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (b *VerbatimString) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, VerbatimStringPrefix, true); err != nil {
		return err
	}

	n, err := bytesutil.ReadIntDelim(br)
	if err != nil {
		return err
	} else if n < 4 {
		return fmt.Errorf("invalid verbatim string length: %d", n)
	}

	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = bytesutil.Expand(*scratch, int(n), false)
	if _, err := io.ReadFull(br, *scratch); err != nil {
		return err
	}

	b.Format = string((*scratch)[:3])
	b.S = string((*scratch)[4:])
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// BigNumber represents the big number type in the RESP protocol. Marshaling a
// nil I value will cause a panic.
type BigNumber struct {
	I *big.Int
}

// MarshalRESP implements the Marshaler method.
func (b BigNumber) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, byte(BigNumberPrefix))
	*scratch = b.I.Append(*scratch, 10)
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (b *BigNumber) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, BigNumberPrefix, true); err != nil {
		return err
	}

	byt, err := bytesutil.ReadBytesDelim(br)
	if err != nil {
		return err
	} else if b.I == nil {
		b.I = new(big.Int)
	}

	var ok bool
	if b.I, ok = b.I.SetString(string(byt), 10); !ok {
		return fmt.Errorf("malformed big number: %q", byt)
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// BulkReader is like BlobString, but it only supports marshalling and will use
// the given LenReader to do so. If LR is nil then the nil bulk string RESP will
// be written
type BulkReader struct {
	LR resp.LenReader
}

// MarshalRESP implements the Marshaler method.
func (b BulkReader) MarshalRESP(w io.Writer) error {
	if b.LR == nil {
		_, err := w.Write(nullRESP2Bulktring)
		return err
	}

	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	l := b.LR.Len()
	*scratch = append(*scratch, byte(BlobStringPrefix))
	*scratch = strconv.AppendInt(*scratch, l, 10)
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	if err != nil {
		return err
	}

	if _, err := io.CopyN(w, b.LR, l); err != nil {
		return err
	} else if _, err := w.Write(delim); err != nil {
		return err
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

func marshalAggHeader(w io.Writer, prefix Prefix, n int, streamHeader bool) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	*scratch = append(*scratch, byte(prefix))
	if streamHeader {
		*scratch = append(*scratch, streamHeaderSize...)
	} else {
		*scratch = strconv.AppendInt(*scratch, int64(n), 10)
	}
	*scratch = append(*scratch, delim...)
	_, err := w.Write(*scratch)
	return err
}

func unmarshalAggHeader(br *bufio.Reader, prefix Prefix, n *int, streamHeader *bool, discardAttr bool) error {
	if err := readAndAssertPrefix(br, prefix, discardAttr); err != nil {
		return err
	}

	b, err := bytesutil.ReadBytesDelim(br)
	if err != nil {
		return err
	} else if streamHeader != nil {
		if *streamHeader = bytes.Equal(b, streamHeaderSize); *streamHeader {
			return nil
		}
	}

	n64, err := bytesutil.ParseInt(b)
	if err != nil {
		return err
	} else if n64 < 0 {
		return fmt.Errorf("invalid number of elements: %d", n64)
	}

	*n = int(n64)
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// ArrayHeader represents the header sent preceding array elements in the RESP
// protocol. It does not actually encompass any elements itself, it only
// declares how many elements will come after it.
//
// ArrayHeader can also be used as the header of a streamed array, whose size is
// not known in advance. When used in that way the StreamedAggregatedElement
// type should be used to the stream's elements.
type ArrayHeader struct {
	NumElems int

	// StreamedArrayHeader indicates that this message is the header message of
	// a streamed array. It is mutually exclusive with NumElems.
	StreamedArrayHeader bool
}

// MarshalRESP implements the Marshaler method.
func (h ArrayHeader) MarshalRESP(w io.Writer) error {
	return marshalAggHeader(w, ArrayPrefix, h.NumElems, h.StreamedArrayHeader)
}

// UnmarshalRESP implements the Unmarshaler method.
func (h *ArrayHeader) UnmarshalRESP(br *bufio.Reader) error {
	return unmarshalAggHeader(br, ArrayPrefix, &h.NumElems, &h.StreamedArrayHeader, true)
}

////////////////////////////////////////////////////////////////////////////////

// MapHeader represents the header sent preceding map key/value pairs in the
// RESP protocol. It does not actually encompass any pairs itself, it only
// declares how many pairs will come after it.
//
// MapHeader can also be used as the header of a streamed map, whose size is
// not known in advance. When used in that way the StreamedAggregatedElement
// type should be used to the stream's elements.
type MapHeader struct {
	NumPairs int

	// StreamedMapHeader indicates that this message is the header message of
	// a streamed map. It is mutually exclusive with NumPairs.
	StreamedMapHeader bool
}

// MarshalRESP implements the Marshaler method.
func (h MapHeader) MarshalRESP(w io.Writer) error {
	return marshalAggHeader(w, MapPrefix, h.NumPairs, h.StreamedMapHeader)
}

// UnmarshalRESP implements the Unmarshaler method.
func (h *MapHeader) UnmarshalRESP(br *bufio.Reader) error {
	return unmarshalAggHeader(br, MapPrefix, &h.NumPairs, &h.StreamedMapHeader, true)
}

////////////////////////////////////////////////////////////////////////////////

// SetHeader represents the header sent preceding set elements in the RESP
// protocol. It does not actually encompass any elements itself, it only
// declares how many elements will come after it.
//
// SetHeader can also be used as the header of a streamed set, whose size is
// not known in advance. When used in that way the StreamedAggregatedElement
// type should be used to the stream's elements.
type SetHeader struct {
	NumElems int

	// StreamedSetHeader indicates that this message is the header message of
	// a streamed set. It is mutually exclusive with NumElems.
	StreamedSetHeader bool
}

// MarshalRESP implements the Marshaler method.
func (h SetHeader) MarshalRESP(w io.Writer) error {
	return marshalAggHeader(w, SetPrefix, h.NumElems, h.StreamedSetHeader)
}

// UnmarshalRESP implements the Unmarshaler method.
func (h *SetHeader) UnmarshalRESP(br *bufio.Reader) error {
	return unmarshalAggHeader(br, SetPrefix, &h.NumElems, &h.StreamedSetHeader, true)
}

////////////////////////////////////////////////////////////////////////////////

// AttributeHeader represents the header sent preceding attribute key/value
// pairs in the RESP protocol. It does not actually encompass any pairs itself,
// it only declares how many pairs will come after it.
type AttributeHeader struct {
	NumPairs int
}

// MarshalRESP implements the Marshaler method.
func (h AttributeHeader) MarshalRESP(w io.Writer) error {
	return marshalAggHeader(w, AttributePrefix, h.NumPairs, false)
}

// UnmarshalRESP implements the Unmarshaler method.
func (h *AttributeHeader) UnmarshalRESP(br *bufio.Reader) error {
	return unmarshalAggHeader(br, AttributePrefix, &h.NumPairs, nil, false)
}

////////////////////////////////////////////////////////////////////////////////

// PushHeader represents the header sent preceding push elements in the RESP
// protocol. It does not actually encompass any elements itself, it only
// declares how many elements will come after it.
type PushHeader struct {
	NumElems int
}

// MarshalRESP implements the Marshaler method.
func (h PushHeader) MarshalRESP(w io.Writer) error {
	return marshalAggHeader(w, PushPrefix, h.NumElems, false)
}

// UnmarshalRESP implements the Unmarshaler method.
func (h *PushHeader) UnmarshalRESP(br *bufio.Reader) error {
	return unmarshalAggHeader(br, PushPrefix, &h.NumElems, nil, true)
}

////////////////////////////////////////////////////////////////////////////////

// StreamedStringChunkBytes represents a chunk of a streamed string in the RESP
// protocol using a byte slice. An empty string indicates the end of the
// streamed string. A B value of nil is an empty string.
type StreamedStringChunkBytes struct {
	B []byte
}

// MarshalRESP implements the Marshaler method.
func (b StreamedStringChunkBytes) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	l := int64(len(b.B))
	*scratch = append(*scratch, byte(StreamedStringChunkPrefix))
	*scratch = strconv.AppendInt(*scratch, l, 10)
	*scratch = append(*scratch, delim...)
	if l > 0 {
		*scratch = append(*scratch, b.B...)
		*scratch = append(*scratch, delim...)
	}
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (b *StreamedStringChunkBytes) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, StreamedStringChunkPrefix, true); err != nil {
		return err
	}

	n, err := bytesutil.ReadIntDelim(br)
	if err != nil {
		return err
	} else if n < 0 {
		return fmt.Errorf("invalid streamed string chunk length: %d", n)
	} else if n == 0 {
		b.B = nil
	} else {
		b.B = bytesutil.Expand(b.B, int(n), false)
		if _, err := io.ReadFull(br, b.B); err != nil {
			return err
		} else if _, err := bytesutil.ReadBytesDelim(br); err != nil {
			return err
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

// StreamedStringChunk represents a chunk of a streamed string in the RESP
// protocol using a go string. An empty string indicates the end of the streamed
// string.
type StreamedStringChunk struct {
	S string
}

// MarshalRESP implements the Marshaler method.
func (b StreamedStringChunk) MarshalRESP(w io.Writer) error {
	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	l := int64(len(b.S))
	*scratch = append(*scratch, byte(StreamedStringChunkPrefix))
	*scratch = strconv.AppendInt(*scratch, l, 10)
	*scratch = append(*scratch, delim...)
	if l > 0 {
		*scratch = append(*scratch, b.S...)
		*scratch = append(*scratch, delim...)
	}
	_, err := w.Write(*scratch)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (b *StreamedStringChunk) UnmarshalRESP(br *bufio.Reader) error {
	if err := readAndAssertPrefix(br, StreamedStringChunkPrefix, true); err != nil {
		return err
	}

	n, err := bytesutil.ReadIntDelim(br)
	if err != nil {
		return err
	} else if n < 0 {
		return fmt.Errorf("invalid streamed string chunk length: %d", n)
	} else if n == 0 {
		b.S = ""
	} else {
		scratch := bytesutil.GetBytes()
		defer bytesutil.PutBytes(scratch)

		*scratch = bytesutil.Expand(*scratch, int(n), false)
		if _, err := io.ReadFull(br, *scratch); err != nil {
			return err
		} else if _, err := bytesutil.ReadBytesDelim(br); err != nil {
			return err
		}
		b.S = string(*scratch)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

// StreamedStringReader implements reading a streamed string RESP message off
// the wire and writing the data being streamed onto the given io.Writer. The
// UnmarshalRESP method will block until the full streamed string has been read
// off and written to the given io.Writer.
type StreamedStringReader struct {
	W io.Writer
}

// UnmarshalRESP implements the Unmarshaler method.
func (r StreamedStringReader) UnmarshalRESP(br *bufio.Reader) error {
	if err := peekAndAssertPrefix(br, StreamedStringChunkPrefix, true); errors.As(err, new(errUnexpectedPrefix)) {
		// the first message in a stream will be a blob string with a size of
		// "?". Discard that message if it comes up.
		//
		// Technically this would also discard a "?" sized blob string which
		// showed up in the middle of a stream, as well as allow a streamed
		// string with no prefix at all. Not ideal, but it would require some
		// state on the StreamedStringReader to fix it, which we don't do for
		// any other Unmarshalers.
		if err := peekAndAssertPrefix(br, BlobStringPrefix, true); err != nil {
			return err
		}
		var u BlobStringBytes
		if err := u.UnmarshalRESP(br); err != nil {
			return err
		} else if !u.StreamedStringHeader {
			return errors.New("sized blob string received instead of an unknown sized streamed string")
		}
	}

	scratch := bytesutil.GetBytes()
	defer bytesutil.PutBytes(scratch)

	chunkBytes := StreamedStringChunkBytes{B: *scratch}
	for {
		if err := chunkBytes.UnmarshalRESP(br); err != nil {
			return err
		} else if len(chunkBytes.B) == 0 {
			return nil
		} else if _, err := r.W.Write(chunkBytes.B); err != nil {
			return err
		}
	}
}

// StreamedStringWriter implements reading raw bytes off of a given io.Reader
// and writing them to an io.Writer as a RESP streamed string message. The
// MarshalRESP method will block until the given io.Reader has returned io.EOF.
//
// TODO This needs tests
type StreamedStringWriter struct {
	R io.Reader

	// Buffer will be used for io.Read calls on the given io.Reader. If nil then
	// a buffer of reasonable size will retrieved from a pool and be used
	// instead.
	Buffer []byte
}

// MarshalRESP implements the Marshaler method.
func (sw *StreamedStringWriter) MarshalRESP(w io.Writer) error {
	if err := (BlobStringBytes{StreamedStringHeader: true}).MarshalRESP(w); err != nil {
		return err
	}

	scratch := &sw.Buffer
	if len(*scratch) == 0 {
		scratch = bytesutil.GetBytes()
		defer bytesutil.PutBytes(scratch)
		*scratch = bytesutil.Expand(*scratch, 1024, false)
	}

	for {
		if _, err := sw.R.Read(*scratch); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		} else if (StreamedStringChunkBytes{B: *scratch}).MarshalRESP(w); err != nil {
			return err
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

// StreamedAggregatedElement is used to unmarshal the elements of a streamed
// aggregated type such that it is possible to check if the end of the stream
// has been reached.
type StreamedAggregatedElement struct {
	// Unmarshaler is used when the message being read isn't the streamed
	// aggregated end type.
	resp.Unmarshaler

	// End is used when UnmarshalRESP is called to indicate that the message
	// read was the streamed aggregated end type (".\r\n") and so the end of the
	// stream has been reached. If this is true then UnmarshalRESP was not
	// called on the Unmarshaler field.
	End bool
}

// UnmarshalRESP implements the Unmarshaler method.
func (s *StreamedAggregatedElement) UnmarshalRESP(br *bufio.Reader) error {
	b, err := br.Peek(len(streamAggEnd))
	if err != nil {
		return err
	} else if s.End = bytes.Equal(b, streamAggEnd); s.End {
		_, err = br.Discard(len(b))
		return err
	}
	return s.Unmarshaler.UnmarshalRESP(br)
}

// StreamedAggregatedEnd is used to marshal the streamed aggregated end type
// message.
type StreamedAggregatedEnd struct{}

// MarshalRESP implements the Marshaler method.
func (s StreamedAggregatedEnd) MarshalRESP(w io.Writer) error {
	_, err := w.Write(streamAggEnd)
	return err
}

////////////////////////////////////////////////////////////////////////////////

// left may be negative to indicate that elements should be discarded until a
// streamed aggregated end type message is encountered.
func discardMultiAfterErr(br *bufio.Reader, left int, lastErr error) error {
	// if the last error which occurred didn't discard the message it was on, we
	// can't do anything
	if !errors.As(lastErr, new(resp.ErrConnUsable)) {
		return lastErr
	} else if err := discardMulti(br, left); err != nil {
		return err
	}

	// The original error was already wrapped in an ErrConnUsable, so just
	// return it as it was given
	return lastErr
}

// Any represents any primitive go type, such as integers, floats, strings,
// bools, etc... It also includes encoding.Text(Un)Marshalers and
// encoding.(Un)BinaryMarshalers. It will _not_ marshal resp.Marshalers.
//
// Most things will be treated as bulk strings, except for those that have their
// own corresponding type in the RESP protocol (e.g. ints). strings and []bytes
// will always be encoded as bulk strings, never simple strings.
//
// Arrays and slices will be treated as RESP arrays, and their values will be
// treated as if also wrapped in an Any struct. Maps will be similarly treated,
// but they will be flattened into arrays of their alternating keys/values
// first.
//
// When using UnmarshalRESP the value of I must be a pointer or nil. If it is
// nil then the RESP value will be read and discarded.
//
// If an error type is read in the UnmarshalRESP method then a resp2.Error will
// be returned with that error, and the value of I won't be touched.
type Any struct {
	I interface{}

	// If true then the MarshalRESP method will marshal all non-array types as
	// bulk strings. This primarily effects integers and errors.
	MarshalBlobString bool

	// If true then no array headers will be sent when MarshalRESP is called.
	// For I values which are non-arrays this means no behavior change. For
	// arrays and embedded arrays it means only the array elements will be
	// written, and an ArrayHeader must have been manually marshalled
	// beforehand.
	MarshalNoArrayHeaders bool
}

func (a Any) cp(i interface{}) Any {
	a.I = i
	return a
}

var byteSliceT = reflect.TypeOf([]byte{})

// NumElems returns the number of non-array elements which would be marshalled
// based on I. For example:
//
//	Any{I: "foo"}.NumElems() == 1
//	Any{I: []string{}}.NumElems() == 0
//	Any{I: []string{"foo"}}.NumElems() == 1
//	Any{I: []string{"foo", "bar"}}.NumElems() == 2
//	Any{I: [][]string{{"foo"}, {"bar", "baz"}, {}}}.NumElems() == 3
//
func (a Any) NumElems() int {
	return numElems(reflect.ValueOf(a.I))
}

var (
	lenReaderT               = reflect.TypeOf(new(resp.LenReader)).Elem()
	encodingTextMarshalerT   = reflect.TypeOf(new(encoding.TextMarshaler)).Elem()
	encodingBinaryMarshalerT = reflect.TypeOf(new(encoding.BinaryMarshaler)).Elem()
)

func numElems(vv reflect.Value) int {
	if !vv.IsValid() {
		return 1
	}

	tt := vv.Type()
	switch {
	case tt.Implements(lenReaderT):
		return 1
	case tt.Implements(encodingTextMarshalerT):
		return 1
	case tt.Implements(encodingBinaryMarshalerT):
		return 1
	}

	switch vv.Kind() {
	case reflect.Ptr:
		return numElems(reflect.Indirect(vv))
	case reflect.Slice, reflect.Array:
		// TODO does []rune need extra support here?
		if vv.Type() == byteSliceT {
			return 1
		}

		l := vv.Len()
		var c int
		for i := 0; i < l; i++ {
			c += numElems(vv.Index(i))
		}
		return c

	case reflect.Map:
		kkv := vv.MapKeys()
		var c int
		for _, kv := range kkv {
			c += numElems(kv)
			c += numElems(vv.MapIndex(kv))
		}
		return c

	case reflect.Interface:
		return numElems(vv.Elem())

	case reflect.Struct:
		return numElemsStruct(vv, true)

	default:
		return 1
	}
}

// this is separated out of numElems because marshalStruct is only given the
// reflect.Value and needs to know the numElems, so it wouldn't make sense to
// recast to an interface{} to pass into NumElems, it would just get turned into
// a reflect.Value again.
func numElemsStruct(vv reflect.Value, flat bool) int {
	tt := vv.Type()
	l := vv.NumField()
	var c int
	for i := 0; i < l; i++ {
		ft, fv := tt.Field(i), vv.Field(i)
		if ft.Anonymous {
			if fv = reflect.Indirect(fv); fv.IsValid() { // fv isn't nil
				c += numElemsStruct(fv, flat)
			}
			continue
		} else if ft.PkgPath != "" || ft.Tag.Get("redis") == "-" {
			continue // continue
		}

		c++ // for the key
		if flat {
			c += numElems(fv)
		} else {
			c++
		}
	}
	return c
}

// MarshalRESP implements the Marshaler method.
func (a Any) MarshalRESP(w io.Writer) error {

	marshalBlobStr := func(b []byte) error {
		return BlobStringBytes{B: b}.MarshalRESP(w)
	}

	switch at := a.I.(type) {
	case []byte:
		return marshalBlobStr(at)
	case string:
		scratch := bytesutil.GetBytes()
		defer bytesutil.PutBytes(scratch)
		*scratch = append(*scratch, at...)
		return marshalBlobStr(*scratch)
	case bool:
		if at {
			return marshalBlobStr(boolStrs[1])
		}
		return marshalBlobStr(boolStrs[0])
	case float32:
		scratch := bytesutil.GetBytes()
		defer bytesutil.PutBytes(scratch)
		*scratch = strconv.AppendFloat(*scratch, float64(at), 'f', -1, 32)
		return marshalBlobStr(*scratch)
	case float64:
		scratch := bytesutil.GetBytes()
		defer bytesutil.PutBytes(scratch)
		*scratch = strconv.AppendFloat(*scratch, at, 'f', -1, 64)
		return marshalBlobStr(*scratch)
	case nil:
		panic("TODO")
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		at64 := bytesutil.AnyIntToInt64(at)
		if a.MarshalBlobString {
			scratch := bytesutil.GetBytes()
			defer bytesutil.PutBytes(scratch)
			*scratch = strconv.AppendInt(*scratch, at64, 10)
			return marshalBlobStr(*scratch)
		}
		return Number{N: at64}.MarshalRESP(w)
	case error:
		if a.MarshalBlobString {
			scratch := bytesutil.GetBytes()
			defer bytesutil.PutBytes(scratch)
			*scratch = append(*scratch, at.Error()...)
			return marshalBlobStr(*scratch)
		}
		// TODO or blob error?
		return SimpleError{S: at.Error()}.MarshalRESP(w)
	case resp.LenReader:
		return BulkReader{LR: at}.MarshalRESP(w)
	case encoding.TextMarshaler:
		b, err := at.MarshalText()
		if err != nil {
			return err
		}
		return marshalBlobStr(b)
	case encoding.BinaryMarshaler:
		b, err := at.MarshalBinary()
		if err != nil {
			return err
		}
		return marshalBlobStr(b)
	}

	// now we use.... reflection! duhduhduuuuh....
	vv := reflect.ValueOf(a.I)

	// if it's a pointer we de-reference and try the pointed to value directly
	if vv.Kind() == reflect.Ptr {
		var ivv reflect.Value
		if vv.IsNil() {
			ivv = reflect.New(vv.Type().Elem())
		} else {
			ivv = reflect.Indirect(vv)
		}
		return a.cp(ivv.Interface()).MarshalRESP(w)
	}

	// some helper functions
	var err error
	var anyWritten bool
	setAnyWritten := func() {
		var errConnUsable resp.ErrConnUsable
		if !errors.As(err, &errConnUsable) {
			anyWritten = true
		}
	}
	arrHeader := func(l int) {
		if a.MarshalNoArrayHeaders || err != nil {
			return
		}
		err = ArrayHeader{NumElems: l}.MarshalRESP(w)
		setAnyWritten()
	}
	arrVal := func(v interface{}) {
		if err != nil {
			return
		}
		err = a.cp(v).MarshalRESP(w)
		setAnyWritten()
	}
	unwrapIfAnyWritten := func() {
		if anyWritten {
			err = resp.ErrConnUnusable(err)
		}
	}

	switch vv.Kind() {
	case reflect.Slice, reflect.Array:
		if vv.IsNil() && !a.MarshalNoArrayHeaders {
			_, err := w.Write(nullRESP2Array)
			return err
		}
		l := vv.Len()
		arrHeader(l)
		for i := 0; i < l; i++ {
			arrVal(vv.Index(i).Interface())
		}
		unwrapIfAnyWritten()

	case reflect.Map:
		if vv.IsNil() && !a.MarshalNoArrayHeaders {
			_, err := w.Write(nullRESP2Array)
			return err
		}
		kkv := vv.MapKeys()
		arrHeader(len(kkv) * 2)
		for _, kv := range kkv {
			arrVal(kv.Interface())
			arrVal(vv.MapIndex(kv).Interface())
		}
		unwrapIfAnyWritten()

	case reflect.Struct:
		return a.marshalStruct(w, vv, false)

	default:
		return resp.ErrConnUsable{
			Err: fmt.Errorf("could not marshal value of type %T", a.I),
		}
	}

	return err
}

func (a Any) marshalStruct(w io.Writer, vv reflect.Value, inline bool) error {
	var err error
	if !a.MarshalNoArrayHeaders && !inline {
		numElems := numElemsStruct(vv, a.MarshalNoArrayHeaders)
		if err = (ArrayHeader{NumElems: numElems}).MarshalRESP(w); err != nil {
			return err
		}
	}

	tt := vv.Type()
	l := vv.NumField()
	for i := 0; i < l; i++ {
		ft, fv := tt.Field(i), vv.Field(i)
		tag := ft.Tag.Get("redis")
		if ft.Anonymous {
			if fv = reflect.Indirect(fv); !fv.IsValid() { // fv is nil
				continue
			} else if err := a.marshalStruct(w, fv, true); err != nil {
				return err
			}
			continue
		} else if ft.PkgPath != "" || tag == "-" {
			continue // unexported
		}

		keyName := ft.Name
		if tag != "" {
			keyName = tag
		}
		if err := (BlobString{S: keyName}).MarshalRESP(w); err != nil {
			return err
		} else if err := a.cp(fv.Interface()).MarshalRESP(w); err != nil {
			return err
		}
	}
	return nil
}

func saneDefault(prefix Prefix) interface{} {
	// we don't handle ErrorPrefix because that always returns an error and
	// doesn't touch I
	switch prefix {
	case ArrayPrefix:
		ii := make([]interface{}, 8)
		return &ii
	case BlobStringPrefix:
		bb := make([]byte, 16)
		return &bb
	case SimpleStringPrefix:
		return new(string)
	case NumberPrefix:
		return new(int64)
	}
	panic("should never get here")
}

// We use pools for these even though they only get used within
// Any.UnmarshalRESP because of how often they get used. Any return from redis
// which has a simple string or bulk string (the vast majority of them) is going
// to go through one of these.
var (
	// RawMessage.UnmarshalInto also uses these
	byteReaderPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewReader(nil)
		},
	}
	bufioReaderPool = sync.Pool{
		New: func() interface{} {
			return bufio.NewReader(nil)
		},
	}
)

// UnmarshalRESP implements the Unmarshaler method.
func (a Any) UnmarshalRESP(br *bufio.Reader) error {
	// if I is itself an Unmarshaler just hit that directly
	if u, ok := a.I.(resp.Unmarshaler); ok {
		return u.UnmarshalRESP(br)
	}

	b, err := br.Peek(1)
	if err != nil {
		return err
	}
	prefix := Prefix(b[0])

	// if the prefix is one of the error types then just parse and return that
	// full message here using the actual unmarshalers, which is easier than
	// re-implementing them.
	switch prefix {
	case SimpleErrorPrefix:
		var into SimpleError
		if err := into.UnmarshalRESP(br); err != nil {
			return err
		}
		return resp.ErrConnUsable{Err: into}
	case BlobErrorPrefix:
		var into BlobError
		if err := into.UnmarshalRESP(br); err != nil {
			return err
		}
		return resp.ErrConnUsable{Err: into}
	}

	// This is a super special case that _must_ be handled before we actually
	// read from the reader. If an *interface{} is given we instead unmarshal
	// into a default (created based on the type of th message), then set the
	// *interface{} to that
	if ai, ok := a.I.(*interface{}); ok {
		innerA := Any{I: saneDefault(prefix)}
		if err := innerA.UnmarshalRESP(br); err != nil {
			return err
		}
		*ai = reflect.ValueOf(innerA.I).Elem().Interface()
		return nil
	}

	br.Discard(1)
	b, err = bytesutil.ReadBytesDelim(br)
	if err != nil {
		return err
	}

	switch prefix {
	case NullPrefix:
		return a.unmarshalNil()

	case ArrayPrefix, MapPrefix, SetPrefix, PushPrefix:
		var l int64
		if len(b) == 1 && b[0] == '?' {
			l = -1
		} else if l, err = bytesutil.ParseInt(b); err != nil {
			return err
		} else if l == -1 {
			return a.unmarshalNil()
		}
		return a.unmarshalAgg(prefix, br, l)

	case BlobStringPrefix, VerbatimStringPrefix:
		var l int64
		if len(b) == 1 && b[0] == '?' {
			l = -1
		} else if l, err = bytesutil.ParseInt(b); err != nil {
			return err
		} else if l == -1 {
			return a.unmarshalNil()
		}

		// if it's a verbatim string then discard the preceding type indicator
		// which is part of it.
		if prefix == VerbatimStringPrefix {
			if l < 4 {
				return errors.New("malformed verbatim string, invalid length")
			} else if _, err := br.Discard(4); err != nil {
				return err
			}
			l -= 4

		} else if l == -1 { // streamed string
			var buf *bytes.Buffer
			var r io.Reader = br
			w, ok := a.I.(io.Writer)
			if !ok {
				// If you're reading this comment and don't want to incur an
				// allocation here then pass in your own io.Writer as the I
				// field.
				buf = new(bytes.Buffer)
				r = buf
				w = buf
			}

			if err := (StreamedStringReader{W: w}).UnmarshalRESP(br); err != nil {
				return err
			} else if ok {
				return nil
			} else {
				return a.unmarshalSingle(r, buf.Len())
			}
		}

		// This is a bit of a clusterfuck. Basically:
		// - If unmarshal returns a non-ErrConnUsable error, return that asap.
		// - If discarding the last 2 bytes (in order to discard the full
		//   message) fails, return that asap
		// - Otherwise return the original error, if there was any
		if err = a.unmarshalSingle(br, int(l)); err != nil {
			if !errors.As(err, new(resp.ErrConnUsable)) {
				return err
			}
		}
		if _, discardErr := br.Discard(2); discardErr != nil {
			return discardErr
		}
		return err

	case BooleanPrefix:
		// convert the f/t boolean body to 0/1, so we're able to have consistent
		// logic related to unmarshaling booleans into ints/floats and
		// unmarshaling ints/floats/strings into booleans.
		if len(b) != 1 {
			return fmt.Errorf("malformed boolean resp body: %q", b)
		} else if b[0] == 't' {
			b[0] = '1'
		} else if b[0] == 'f' {
			b[0] = '0'
		} else {
			return fmt.Errorf("malformed boolean resp body: %q", b)
		}
		fallthrough

	case SimpleStringPrefix, NumberPrefix, DoublePrefix, BigNumberPrefix:
		reader := byteReaderPool.Get().(*bytes.Reader)
		reader.Reset(b)
		err := a.unmarshalSingle(reader, reader.Len())
		byteReaderPool.Put(reader)
		return err

	default:
		return fmt.Errorf("unknown type prefix %q", prefix)
	}
}

func (a Any) unmarshalSingle(body io.Reader, n int) error {
	var (
		err error
		i   int64
		ui  uint64
	)

	switch ai := a.I.(type) {
	case nil:
		// just read it and do nothing. This only catches the case of a.I being
		// actually nil, not a typed nil pointer.
		err = bytesutil.ReadNDiscard(body, n)
	case *string:
		scratch := bytesutil.GetBytes()
		*scratch, err = bytesutil.ReadNAppend(body, *scratch, n)
		*ai = string(*scratch)
		bytesutil.PutBytes(scratch)
	case *[]byte:
		if *ai == nil {
			*ai = []byte{}
		}
		*ai, err = bytesutil.ReadNAppend(body, (*ai)[:0], n)
	case *bool:
		var f float64
		f, err = bytesutil.ReadFloat(body, 64, n)
		*ai = f != 0
		break
	case *int:
		i, err = bytesutil.ReadInt(body, n)
		*ai = int(i)
	case *int8:
		i, err = bytesutil.ReadInt(body, n)
		*ai = int8(i)
	case *int16:
		i, err = bytesutil.ReadInt(body, n)
		*ai = int16(i)
	case *int32:
		i, err = bytesutil.ReadInt(body, n)
		*ai = int32(i)
	case *int64:
		i, err = bytesutil.ReadInt(body, n)
		*ai = i
	case *uint:
		ui, err = bytesutil.ReadUint(body, n)
		*ai = uint(ui)
	case *uint8:
		ui, err = bytesutil.ReadUint(body, n)
		*ai = uint8(ui)
	case *uint16:
		ui, err = bytesutil.ReadUint(body, n)
		*ai = uint16(ui)
	case *uint32:
		ui, err = bytesutil.ReadUint(body, n)
		*ai = uint32(ui)
	case *uint64:
		ui, err = bytesutil.ReadUint(body, n)
		*ai = ui
	case *float32:
		var f float64
		f, err = bytesutil.ReadFloat(body, 32, n)
		*ai = float32(f)
	case *float64:
		*ai, err = bytesutil.ReadFloat(body, 64, n)
	case io.Writer:
		_, err = io.CopyN(ai, body, int64(n))
	case encoding.TextUnmarshaler:
		scratch := bytesutil.GetBytes()
		defer bytesutil.PutBytes(scratch)
		if *scratch, err = bytesutil.ReadNAppend(body, *scratch, n); err != nil {
			break
		}
		err = ai.UnmarshalText(*scratch)
	case encoding.BinaryUnmarshaler:
		scratch := bytesutil.GetBytes()
		defer bytesutil.PutBytes(scratch)
		if *scratch, err = bytesutil.ReadNAppend(body, *scratch, n); err != nil {
			break
		}
		err = ai.UnmarshalBinary(*scratch)
	default:

		discardAndErr := func(fmtStr string, args ...interface{}) {
			scratch := bytesutil.GetBytes()
			defer bytesutil.PutBytes(scratch)
			if *scratch, err = bytesutil.ReadNAppend(body, *scratch, n); err != nil {
				return
			}
			err = fmt.Errorf(fmtStr+", message body was %q", append(args, *scratch))
			err = resp.ErrConnUsable{Err: err}
		}

		// check if the receiver is a non-nil pointer to a pointer, and if so
		// unmarshal into _that_, possibly filling in the inner pointer if it's
		// nil.
		if ptr := reflect.ValueOf(a.I); ptr.Kind() == reflect.Ptr {
			if ptr.IsNil() {
				discardAndErr("can't unmarshal into nil %s", ptr.Type())
				break
			} else if innerPtr := ptr.Elem(); innerPtr.Kind() == reflect.Ptr {
				if innerPtr.IsNil() {
					innerPtr.Set(reflect.New(innerPtr.Type().Elem()))
				}
				return Any{I: innerPtr.Interface()}.unmarshalSingle(body, n)
			}
		}

		discardAndErr("can't unmarshal into %T", a.I)
	}

	return err
}

func (a Any) unmarshalNil() error {
	vv := reflect.ValueOf(a.I)
	if vv.Kind() != reflect.Ptr || !vv.Elem().CanSet() {
		// If the type in I can't be set then just ignore it. This is kind of
		// weird but it's what encoding/json does in the same circumstance
		return nil
	}

	vve := vv.Elem()
	vve.Set(reflect.Zero(vve.Type()))
	return nil
}

func (a Any) maybeUnmarshalRESP(br *bufio.Reader, stream bool) (bool, error) {
	if !stream {
		return true, a.UnmarshalRESP(br)
	}

	// TODO this might cause Any to end up on the heap
	streamAgg := StreamedAggregatedElement{Unmarshaler: &a}
	err := streamAgg.UnmarshalRESP(br)
	return !streamAgg.End, err
}

func (a Any) unmarshalAgg(prefix Prefix, br *bufio.Reader, l int64) error {
	if prefix == MapPrefix {
		l *= 2
	}

	size := int(l)
	stream := size < 0
	if a.I == nil {
		return discardMulti(br, size)
	}

	v := reflect.ValueOf(a.I)
	if v.Kind() != reflect.Ptr {
		err := resp.ErrConnUsable{
			Err: fmt.Errorf("can't unmarshal resp %s into %T", prefix, a.I),
		}
		return discardMultiAfterErr(br, size, err)
	}

	for ; v.Kind() == reflect.Ptr; v = reflect.Indirect(v) {
		// this loop de-references as many pointers as possible.
	}

	switch v.Kind() {
	case reflect.Slice:
		slice := v
		if size > slice.Cap() || slice.IsNil() {
			sliceSize := size
			if stream {
				sliceSize = 8
			}
			slice.Set(reflect.MakeSlice(slice.Type(), 0, sliceSize))
		} else {
			slice.SetLen(0)
		}

		// TODO this isn't ideal, but it works for now. Ideally this loop would
		// be unmarshaling straight into slice elements based on i, expanding
		// slice as needed, before finally setting the length appropriately at
		// the end.
		into := reflect.New(v.Type().Elem())
		for i := 0; i < size || stream; i++ {
			into.Elem().Set(reflect.Zero(into.Type().Elem()))
			anyInto := Any{I: into.Interface()}
			if more, err := anyInto.maybeUnmarshalRESP(br, stream); err != nil {
				return discardMultiAfterErr(br, size-i-1, err)
			} else if !more {
				break
			}
			slice = reflect.Append(slice, into.Elem())
		}
		v.Set(slice)
		return nil

	case reflect.Map:
		if !stream && size%2 != 0 {
			err := resp.ErrConnUsable{Err: fmt.Errorf("cannot decode resp %s with odd number of elements into map", prefix)}
			return discardMultiAfterErr(br, size, err)
		} else if v.IsNil() {
			mapSize := size
			if stream {
				mapSize = 3
			}
			v.Set(reflect.MakeMapWithSize(v.Type(), mapSize))
		} else {
			for _, key := range v.MapKeys() {
				v.SetMapIndex(key, reflect.Value{})
			}
		}

		var kvs reflect.Value
		if size > 0 && canShareReflectValue(v.Type().Key()) {
			kvs = reflect.New(v.Type().Key())
		}

		var vvs reflect.Value
		if size > 0 && canShareReflectValue(v.Type().Elem()) {
			vvs = reflect.New(v.Type().Elem())
		}

		for i := 0; i < size || stream; i += 2 {
			kv := kvs
			if !kv.IsValid() {
				kv = reflect.New(v.Type().Key())
			}
			if more, err := (Any{I: kv.Interface()}).maybeUnmarshalRESP(br, stream); err != nil {
				return discardMultiAfterErr(br, size-i-1, err)
			} else if !more {
				break
			}

			vv := vvs
			if !vv.IsValid() {
				vv = reflect.New(v.Type().Elem())
			}
			if err := (Any{I: vv.Interface()}).UnmarshalRESP(br); err != nil {
				return discardMultiAfterErr(br, int(l)-i-2, err)
			}

			v.SetMapIndex(kv.Elem(), vv.Elem())
		}
		return nil

	case reflect.Struct:
		if !stream && size%2 != 0 {
			err := resp.ErrConnUsable{Err: fmt.Errorf("cannot decode resp %s with odd number of elements into struct", prefix)}
			return discardMultiAfterErr(br, size, err)
		}

		structFields := getStructFields(v.Type())
		var field string

		for i := 0; i < size || stream; i += 2 {
			if more, err := (Any{I: &field}).maybeUnmarshalRESP(br, stream); err != nil {
				return discardMultiAfterErr(br, size-i-1, err)
			} else if !more {
				break
			}

			var vv reflect.Value
			structField, ok := structFields[field]
			if ok {
				vv = getStructField(v, structField.indices)
			}

			if !ok || !vv.IsValid() {
				// discard the value
				if err := (Any{}).UnmarshalRESP(br); err != nil {
					return discardMultiAfterErr(br, size-i-2, err)
				}
				continue
			}

			if err := (Any{I: vv.Interface()}).UnmarshalRESP(br); err != nil {
				return discardMultiAfterErr(br, size-i-2, err)
			}
		}

		return nil

	default:
		err := resp.ErrConnUsable{Err: fmt.Errorf("cannot decode resp %s into %v", prefix, v.Type())}
		return discardMultiAfterErr(br, int(l), err)
	}
}

func canShareReflectValue(ty reflect.Type) bool {
	switch ty.Kind() {
	case reflect.Bool,
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Uintptr,
		reflect.Float32,
		reflect.Float64,
		reflect.Complex64,
		reflect.Complex128,
		reflect.String:
		return true
	default:
		return false
	}
}

type structField struct {
	name    string
	fromTag bool // from a tag overwrites a field name
	indices []int
}

// encoding/json uses a similar pattern for unmarshaling into structs
var structFieldsCache sync.Map // aka map[reflect.Type]map[string]structField

func getStructFields(t reflect.Type) map[string]structField {
	if mV, ok := structFieldsCache.Load(t); ok {
		return mV.(map[string]structField)
	}

	getIndices := func(parents []int, i int) []int {
		indices := make([]int, len(parents), len(parents)+1)
		copy(indices, parents)
		indices = append(indices, i)
		return indices
	}

	m := map[string]structField{}

	var populateFrom func(reflect.Type, []int)
	populateFrom = func(t reflect.Type, parents []int) {
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		l := t.NumField()

		// first get all fields which aren't embedded structs
		for i := 0; i < l; i++ {
			ft := t.Field(i)
			if ft.Anonymous || ft.PkgPath != "" {
				continue
			}

			key, fromTag := ft.Name, false
			if tag := ft.Tag.Get("redis"); tag != "" && tag != "-" {
				key, fromTag = tag, true
			}
			if m[key].fromTag {
				continue
			}
			m[key] = structField{
				name:    key,
				fromTag: fromTag,
				indices: getIndices(parents, i),
			}
		}

		// then find all embedded structs and descend into them
		for i := 0; i < l; i++ {
			ft := t.Field(i)
			if !ft.Anonymous {
				continue
			}
			populateFrom(ft.Type, getIndices(parents, i))
		}
	}

	populateFrom(t, []int{})
	structFieldsCache.LoadOrStore(t, m)
	return m
}

// v must be setable. Always returns a Kind() == reflect.Ptr, unless it returns
// the zero Value, which means a setable value couldn't be gotten.
func getStructField(v reflect.Value, ii []int) reflect.Value {
	if len(ii) == 0 {
		return v.Addr()
	}
	i, ii := ii[0], ii[1:]

	iv := v.Field(i)
	if iv.Kind() == reflect.Ptr && iv.IsNil() {
		// If the field is a pointer to an unexported type then it won't be
		// settable, though if the user pre-sets the value it will be (I think).
		if !iv.CanSet() {
			return reflect.Value{}
		}
		iv.Set(reflect.New(iv.Type().Elem()))
	}
	iv = reflect.Indirect(iv)

	return getStructField(iv, ii)
}

////////////////////////////////////////////////////////////////////////////////

// RawMessage is a Marshaler/Unmarshaler which will capture the exact raw bytes
// of a RESP message. When Marshaling the exact bytes of the RawMessage will be
// written as-is. When Unmarshaling the bytes of a single RESP message will be
// read into the RawMessage's bytes.
type RawMessage []byte

// MarshalRESP implements the Marshaler method.
func (rm RawMessage) MarshalRESP(w io.Writer) error {
	_, err := w.Write(rm)
	return err
}

// UnmarshalRESP implements the Unmarshaler method.
func (rm *RawMessage) UnmarshalRESP(br *bufio.Reader) error {
	*rm = (*rm)[:0]
	return rm.unmarshal(br)
}

func (rm *RawMessage) unmarshal(br *bufio.Reader) error {
	b, err := br.ReadSlice('\n')
	if err != nil {
		return err
	}
	*rm = append(*rm, b...)

	if len(b) < 3 {
		return errors.New("malformed data read")
	}
	body := b[1 : len(b)-2]

	switch Prefix(b[0]) {
	case ArrayPrefix:
		l, err := bytesutil.ParseInt(body)
		if err != nil {
			return err
		} else if l == -1 {
			return nil
		}
		for i := 0; i < int(l); i++ {
			if err := rm.unmarshal(br); err != nil {
				return err
			}
		}
		return nil
	case BlobStringPrefix:
		l, err := bytesutil.ParseInt(body) // fuck DRY
		if err != nil {
			return err
		} else if l == -1 {
			return nil
		}
		*rm, err = bytesutil.ReadNAppend(br, *rm, int(l+2))
		return err
	case SimpleErrorPrefix, SimpleStringPrefix, NumberPrefix:
		return nil
	default:
		return fmt.Errorf("unknown type prefix %q", b[0])
	}
}

// UnmarshalInto is a shortcut for wrapping this RawMessage in a *bufio.Reader
// and passing that into the given Unmarshaler's UnmarshalRESP method. Any error
// from calling UnmarshalRESP is returned, and the RawMessage is unaffected in
// all cases.
func (rm RawMessage) UnmarshalInto(u resp.Unmarshaler) error {
	r := byteReaderPool.Get().(*bytes.Reader)
	r.Reset(rm)
	br := bufioReaderPool.Get().(*bufio.Reader)
	br.Reset(r)
	err := u.UnmarshalRESP(br)
	bufioReaderPool.Put(br)
	byteReaderPool.Put(r)
	return err
}

// IsNil returns true if the contents of RawMessage are one of the nil values.
func (rm RawMessage) IsNil() bool {
	// TODO this is wrong
	return bytes.Equal(rm, nullRESP2Bulktring) || bytes.Equal(rm, nullRESP2Array)
}

// IsEmptyArray returns true if the contents of RawMessage is empty array value.
func (rm RawMessage) IsEmptyArray() bool {
	return bytes.Equal(rm, emptyArray)
}
