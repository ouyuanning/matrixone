// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bytejson

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"encoding/json"
	"math"
	"math/bits"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
)

func ParseFromString(s string) (ret ByteJson, err error) {
	if len(s) == 0 {
		err = moerr.NewInvalidInputNoCtxf("json text %s", s)
		return
	}
	data := util.UnsafeStringToBytes(s)
	ret, err = ParseFromByteSlice(data)
	return
}

func ParseFromByteSlice(s []byte) (bj ByteJson, err error) {
	if len(s) == 0 {
		err = moerr.NewInvalidInputNoCtxf("json text %s", string(s))
		return
	}
	if !json.Valid(s) {
		err = moerr.NewInvalidInputNoCtxf("json text %s", string(s))
		return
	}
	err = bj.UnmarshalJSON(s)
	return
}

func toString(buf, data []byte) ([]byte, error) {
	return appendString(buf, util.UnsafeBytesToString(data))
}

// extend slice to have n zero bytes
func extendByte(buf []byte, n int) []byte {
	buf = append(buf, make([]byte, n)...)
	return buf
}

func addString(buf []byte, in string) []byte {
	off := len(buf)
	//encoding length
	buf = extendByte(buf, binary.MaxVarintLen64)
	inLen := binary.PutUvarint(buf[off:], uint64(len(in)))
	//cut length
	buf = buf[:off+inLen]
	//add string
	buf = append(buf, in...)
	return buf
}

func checkFloat64(n float64) error {
	if math.IsInf(n, 0) || math.IsNaN(n) {
		return moerr.NewInvalidInputNoCtxf("json float64 %f", n)
	}
	return nil
}

func calStrLen(buf []byte) (int, int) {
	strLen, lenLen := uint64(buf[0]), 1
	if strLen >= utf8.RuneSelf {
		strLen, lenLen = binary.Uvarint(buf)
	}
	return int(strLen), lenLen
}

func isIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if (i != 0 && s[i] >= '0' && s[i] <= '9') ||
			(s[i] >= 'a' && s[i] <= 'z') || (s[i] >= 'A' && s[i] <= 'Z') ||
			s[i] == '_' || s[i] == '$' || s[i] >= 0x80 {
			continue
		}
		return false
	}
	return true
}

func ParseJsonPath(path string) (p Path, err error) {
	pg := newPathGenerator(path)
	pg.trimSpace()
	if !pg.hasNext() || pg.next() != '$' {
		err = moerr.NewInvalidInputNoCtxf("invalid json path '%s'", path)
	}
	pg.trimSpace()
	subPaths := make([]subPath, 0, 8)
	var ok bool
	for pg.hasNext() {
		switch pg.front() {
		case '.':
			subPaths, ok = pg.generateKey(subPaths)
		case '[':
			subPaths, ok = pg.generateIndex(subPaths)
		case '*':
			subPaths, ok = pg.generateDoubleStar(subPaths)
		default:
			ok = false
		}
		if !ok {
			err = moerr.NewInvalidInputNoCtxf("invalid json path '%s'", path)
			return
		}
		pg.trimSpace()
	}

	if len(subPaths) > 0 && subPaths[len(subPaths)-1].tp == subPathDoubleStar {
		err = moerr.NewInvalidInputNoCtxf("invalid json path '%s'", path)
		return
	}
	p.init(subPaths)
	return
}

func addByteElem(buf []byte, entryStart int, elems []ByteJson) []byte {
	for i, elem := range elems {
		buf[entryStart+i*valEntrySize] = byte(elem.Type)
		if elem.Type == TpCodeLiteral {
			buf[entryStart+i*valEntrySize+valTypeSize] = elem.Data[0]
		} else {
			endian.PutUint32(buf[entryStart+i*valEntrySize+valTypeSize:], uint32(len(buf)))
			buf = append(buf, elem.Data...)
		}
	}
	return buf
}

func buildJsonObject(keys [][]byte, elems []ByteJson) (ByteJson, error) {
	totalSize := headerSize + len(elems)*(keyEntrySize+valEntrySize)
	for i, elem := range elems {
		if elem.Type != TpCodeLiteral {
			totalSize += len(elem.Data)
		}
		totalSize += len(keys[i])
	}
	buf := make([]byte, headerSize+len(elems)*(keyEntrySize+valEntrySize), totalSize)
	endian.PutUint32(buf, uint32(len(elems)))
	endian.PutUint32(buf[docSizeOff:], uint32(totalSize))
	for i, key := range keys {
		endian.PutUint32(buf[headerSize+i*keyEntrySize:], uint32(len(buf)))
		endian.PutUint16(buf[headerSize+i*keyEntrySize+keyOriginOff:], uint16(len(key)))
		buf = append(buf, key...)
	}
	entryStart := headerSize + len(elems)*keyEntrySize
	buf = addByteElem(buf, entryStart, elems)
	return ByteJson{Type: TpCodeObject, Data: buf}, nil
}

func buildBinaryJSONArray(elems []ByteJson) ByteJson {
	totalSize := headerSize + len(elems)*valEntrySize
	for _, elem := range elems {
		if elem.Type != TpCodeLiteral {
			totalSize += len(elem.Data)
		}
	}
	buf := make([]byte, headerSize+len(elems)*valEntrySize, totalSize)
	endian.PutUint32(buf, uint32(len(elems)))
	endian.PutUint32(buf[docSizeOff:], uint32(totalSize))
	buf = buildBinaryJSONElements(buf, headerSize, elems)
	return ByteJson{Type: TpCodeArray, Data: buf}
}

func buildBinaryJSONElements(buf []byte, entryStart int, elems []ByteJson) []byte {
	for i, elem := range elems {
		buf[entryStart+i*valEntrySize] = elem.Type
		if elem.Type == TpCodeLiteral {
			buf[entryStart+i*valEntrySize+valTypeSize] = elem.Data[0]
		} else {
			endian.PutUint32(buf[entryStart+i*valEntrySize+valTypeSize:], uint32(len(buf)))
			buf = append(buf, elem.Data...)
		}
	}
	return buf
}

func mergeToArray(origin []ByteJson) ByteJson {
	totalSize := headerSize + len(origin)*valEntrySize
	for _, el := range origin {
		if el.Type != TpCodeLiteral {
			totalSize += len(el.Data)
		}
	}
	buf := make([]byte, headerSize+len(origin)*valEntrySize, totalSize)
	endian.PutUint32(buf, uint32(len(origin)))
	endian.PutUint32(buf[docSizeOff:], uint32(totalSize))
	buf = addByteElem(buf, headerSize, origin)
	return ByteJson{Type: TpCodeArray, Data: buf}
}

// check unnest mode
func checkMode(mode string) bool {
	if mode == "both" || mode == "array" || mode == "object" {
		return true
	}
	return false
}

func genIndexOrKey(pathStr string) ([]byte, []byte) {
	if pathStr[len(pathStr)-1] == ']' {
		// find last '['
		idx := strings.LastIndex(pathStr, "[")
		return util.UnsafeStringToBytes(pathStr[idx : len(pathStr)-1]), nil
	}
	// find last '.'
	idx := strings.LastIndex(pathStr, ".")
	return nil, util.UnsafeStringToBytes(pathStr[idx+1:])
}

// for test
func (r UnnestResult) String() string {
	var buf bytes.Buffer
	if val, ok := r["key"]; ok && val != nil {
		buf.WriteString("key: ")
		buf.WriteString(string(val) + ", ")
	}
	if val, ok := r["path"]; ok && val != nil {
		buf.WriteString("path: ")
		buf.WriteString(string(val) + ", ")
	}
	if val, ok := r["index"]; ok && val != nil {
		buf.WriteString("index: ")
		buf.WriteString(string(val) + ", ")
	}
	if val, ok := r["value"]; ok && val != nil {
		buf.WriteString("value: ")
		bj := ByteJson{}
		bj.Unmarshal(val)
		val, _ = bj.MarshalJSON()
		buf.WriteString(string(val) + ", ")
	}
	if val, ok := r["this"]; ok && val != nil {
		buf.WriteString("this: ")
		bj := ByteJson{}
		bj.Unmarshal(val)
		val, _ = bj.MarshalJSON()
		buf.WriteString(string(val))
	}
	return buf.String()
}

func checkAllNull(vals []ByteJson) bool {
	allNull := true
	for _, val := range vals {
		if !val.IsNull() {
			allNull = false
			break
		}
	}
	return allNull
}

// NumberParts is the result of parsing out a valid JSON number. It contains
// the parts of a number. The parts are used for integer conversion.
type NumberParts struct {
	Neg  bool
	Intp []byte
	Frac []byte
	Exp  []byte
}

// ParseNumber constructs numberParts from given []byte. The logic here is
// similar to consumeNumber above with the difference of having to construct
// numberParts. The slice fields in numberParts are subslices of the input.
func ParseNumberParts(input []byte) (NumberParts, bool) {
	var neg bool
	var intp []byte
	var frac []byte
	var exp []byte

	s := input
	if len(s) == 0 {
		return NumberParts{}, false
	}

	// Optional -
	if s[0] == '-' {
		neg = true
		s = s[1:]
		if len(s) == 0 {
			return NumberParts{}, false
		}
	}

	// Digits
	switch {
	case s[0] == '0':
		// Skip first 0 and no need to store.
		s = s[1:]

	case '1' <= s[0] && s[0] <= '9':
		intp = s
		n := 1
		s = s[1:]
		for len(s) > 0 && '0' <= s[0] && s[0] <= '9' {
			s = s[1:]
			n++
		}
		intp = intp[:n]

	default:
		return NumberParts{}, false
	}

	// . followed by 1 or more digits.
	if len(s) >= 2 && s[0] == '.' && '0' <= s[1] && s[1] <= '9' {
		frac = s[1:]
		n := 1
		s = s[2:]
		for len(s) > 0 && '0' <= s[0] && s[0] <= '9' {
			s = s[1:]
			n++
		}
		frac = frac[:n]
	}

	// e or E followed by an optional - or + and
	// 1 or more digits.
	if len(s) >= 2 && (s[0] == 'e' || s[0] == 'E') {
		s = s[1:]
		exp = s
		n := 0
		if s[0] == '+' || s[0] == '-' {
			s = s[1:]
			n++
			if len(s) == 0 {
				return NumberParts{}, false
			}
		}
		for len(s) > 0 && '0' <= s[0] && s[0] <= '9' {
			s = s[1:]
			n++
		}
		exp = exp[:n]
	}

	return NumberParts{
		Neg:  neg,
		Intp: intp,
		Frac: bytes.TrimRight(frac, "0"), // Remove unnecessary 0s to the right.
		Exp:  exp,
	}, true
}

// NormalizeToIntString returns an integer string in normal form without the
// E-notation for given numberParts. It will return false if it is not an
// integer or if the exponent exceeds than max/min int value.
func NormalizeToIntString(n NumberParts) (string, bool) {
	intpSize := len(n.Intp)
	fracSize := len(n.Frac)

	if intpSize == 0 && fracSize == 0 {
		return "0", true
	}

	var exp int
	if len(n.Exp) > 0 {
		i, err := strconv.ParseInt(string(n.Exp), 10, 32)
		if err != nil {
			return "", false
		}
		exp = int(i)
	}

	var num []byte
	if exp >= 0 {
		// For positive E, shift fraction digits into integer part and also pad
		// with zeroes as needed.

		// If there are more digits in fraction than the E value, then the
		// number is not an integer.
		if fracSize > exp {
			return "", false
		}

		// Make sure resulting digits are within max value limit to avoid
		// unnecessarily constructing a large byte slice that may simply fail
		// later on.
		const maxDigits = 20 // Max uint64 value has 20 decimal digits.
		if intpSize+exp > maxDigits {
			return "", false
		}

		// Set cap to make a copy of integer part when appended.
		num = n.Intp[:len(n.Intp):len(n.Intp)]
		num = append(num, n.Frac...)
		for i := 0; i < exp-fracSize; i++ {
			num = append(num, '0')
		}
	} else {
		// For negative E, shift digits in integer part out.

		// If there are fractions, then the number is not an integer.
		if fracSize > 0 {
			return "", false
		}

		// index is where the decimal point will be after adjusting for negative
		// exponent.
		index := intpSize + exp
		if index < 0 {
			return "", false
		}

		num = n.Intp
		// If any of the digits being shifted to the right of the decimal point
		// is non-zero, then the number is not an integer.
		for i := index; i < intpSize; i++ {
			if num[i] != '0' {
				return "", false
			}
		}
		num = num[:index]
	}

	if n.Neg {
		return "-" + string(num), true
	}
	return string(num), true
}

// indexNeedEscapeInString returns the index of the character that needs
// escaping. If no characters need escaping, this returns the input length.
func indexNeedEscapeInString(s string) int {
	for i, r := range s {
		if r < ' ' || r == '\\' || r == '"' || r == utf8.RuneError {
			return i
		}
	}
	return len(s)
}

func appendString(out []byte, in string) ([]byte, error) {
	out = append(out, '"')
	i := indexNeedEscapeInString(in)
	in, out = in[i:], append(out, in[:i]...)
	for len(in) > 0 {
		switch r, n := utf8.DecodeRuneInString(in); {
		case r == utf8.RuneError && n == 1:
			return out, moerr.NewInvalidInputNoCtx("invalid UTF-8")
		case r < ' ' || r == '"' || r == '\\':
			out = append(out, '\\')
			switch r {
			case '"', '\\':
				out = append(out, byte(r))
			case '\b':
				out = append(out, 'b')
			case '\f':
				out = append(out, 'f')
			case '\n':
				out = append(out, 'n')
			case '\r':
				out = append(out, 'r')
			case '\t':
				out = append(out, 't')
			default:
				out = append(out, 'u')
				out = append(out, "0000"[1+(bits.Len32(uint32(r))-1)/4:]...)
				out = strconv.AppendUint(out, uint64(r), 16)
			}
			in = in[n:]
		default:
			i := indexNeedEscapeInString(in[n:])
			in, out = in[n+i:], append(out, in[:n+i]...)
		}
	}
	out = append(out, '"')
	return out, nil
}

func CreateByteJSON(in any) (ByteJson, error) {
	return CreateByteJSONWithCheck(in)
}

func CreateByteJSONWithCheck(in any) (ByteJson, error) {
	typeCode, buf, err := appendBinaryJSON(nil, in)
	if err != nil {
		return ByteJson{}, err
	}
	return ByteJson{Type: typeCode, Data: buf}, nil
}

func appendBinaryJSON(buf []byte, in any) (TpCode, []byte, error) {
	var typeCode byte
	var err error
	switch x := in.(type) {
	case nil:
		typeCode = TpCodeLiteral
		buf = append(buf, LiteralNull)
	case bool:
		typeCode = TpCodeLiteral
		if x {
			buf = append(buf, LiteralTrue)
		} else {
			buf = append(buf, LiteralFalse)
		}
	case int64:
		typeCode = TpCodeInt64
		buf = appendBinaryUint64(buf, uint64(x))
	case uint64:
		typeCode = TpCodeUint64
		buf = appendBinaryUint64(buf, x)
	case float64:
		typeCode = TpCodeFloat64
		buf = appendBinaryFloat64(buf, x)
	case json.Number:
		typeCode, buf, err = appendBinaryNumber(buf, x)
		if err != nil {
			return typeCode, nil, err
		}
	case string:
		typeCode = TpCodeString
		buf = appendBinaryString(buf, x)
	case ByteJson:
		typeCode = x.Type
		buf = append(buf, x.Data...)
	case []any:
		typeCode = TpCodeArray
		buf, err = appendBinaryArray(buf, x)
		if err != nil {
			return typeCode, nil, err
		}
	case map[string]any:
		typeCode = TpCodeObject
		buf, err = appendBinaryObject(buf, x)
		if err != nil {
			return typeCode, nil, err
		}
	default:
		return typeCode, nil, moerr.NewInvalidArgNoCtx("invalid json type", reflect.TypeOf(in).String())
	}
	return typeCode, buf, err
}

func appendBinaryUint64(buf []byte, v uint64) []byte {
	off := len(buf)
	buf = appendZero(buf, 8)
	endian.PutUint64(buf[off:], v)
	return buf
}

func appendUint32(buf []byte, v uint32) []byte {
	var tmp [4]byte
	endian.PutUint32(tmp[:], v)
	return append(buf, tmp[:]...)
}

func appendBinaryFloat64(buf []byte, v float64) []byte {
	off := len(buf)
	buf = appendZero(buf, 8)
	endian.PutUint64(buf[off:], math.Float64bits(v))
	return buf
}

func appendBinaryNumber(buf []byte, x json.Number) (TpCode, []byte, error) {
	if strings.Contains(x.String(), "Ee.") {
		f64, err := x.Float64()
		if err != nil {
			return TpCodeFloat64, nil, moerr.NewInvalidArgNoCtx("invalid json number", x.String())
		}
		return TpCodeFloat64, appendBinaryFloat64(buf, f64), nil
	} else if val, err := x.Int64(); err == nil {
		return TpCodeInt64, appendBinaryUint64(buf, uint64(val)), nil
	} else if val, err := strconv.ParseUint(string(x), 10, 64); err == nil {
		return TpCodeUint64, appendBinaryUint64(buf, val), nil
	}
	val, err := x.Float64()
	if err == nil {
		return TpCodeFloat64, appendBinaryFloat64(buf, val), nil
	}
	var typeCode TpCode
	return typeCode, nil, moerr.NewInvalidArgNoCtx("invalid json number", x.String())
}

func appendBinaryString(buf []byte, v string) []byte {
	begin := len(buf)
	buf = appendZero(buf, binary.MaxVarintLen64)
	lenLen := binary.PutUvarint(buf[begin:], uint64(len(v)))
	buf = buf[:len(buf)-binary.MaxVarintLen64+lenLen]
	buf = append(buf, v...)
	return buf
}

func appendBinaryArray(buf []byte, array []any) ([]byte, error) {
	docOff := len(buf)
	buf = appendUint32(buf, uint32(len(array)))
	buf = appendZero(buf, docSizeOff)
	valEntryBegin := len(buf)
	buf = appendZero(buf, len(array)*valEntrySize)
	for i, val := range array {
		var err error
		buf, err = appendBinaryValElem(buf, docOff, valEntryBegin+i*valEntrySize, val)
		if err != nil {
			return nil, moerr.NewInvalidArgNoCtx("invalid json array", val)
		}
	}
	docSize := len(buf) - docOff
	endian.PutUint32(buf[docOff+docSizeOff:], uint32(docSize))
	return buf, nil
}

func appendBinaryObject(buf []byte, x map[string]any) ([]byte, error) {
	docOff := len(buf)
	buf = appendUint32(buf, uint32(len(x)))
	buf = appendZero(buf, docSizeOff)
	keyEntryBegin := len(buf)
	buf = appendZero(buf, len(x)*keyEntrySize)
	valEntryBegin := len(buf)
	buf = appendZero(buf, len(x)*valEntrySize)

	fields := make([]field, 0, len(x))
	for key, val := range x {
		fields = append(fields, field{key: key, val: val})
	}
	slices.SortFunc(fields, func(i, j field) int {
		return cmp.Compare(i.key, j.key)
	})
	for i, field := range fields {
		keyEntryOff := keyEntryBegin + i*keyEntrySize
		keyOff := len(buf) - docOff
		keyLen := uint32(len(field.key))
		if keyLen > math.MaxUint16 {
			return nil, moerr.NewInvalidArgNoCtx("invalid json key", field.key)
		}
		endian.PutUint32(buf[keyEntryOff:], uint32(keyOff))
		endian.PutUint16(buf[keyEntryOff+keyOriginOff:], uint16(keyLen))
		buf = append(buf, field.key...)
	}
	for i, field := range fields {
		var err error
		buf, err = appendBinaryValElem(buf, docOff, valEntryBegin+i*valEntrySize, field.val)
		if err != nil {
			return nil, moerr.NewInvalidArgNoCtx("invalid json object", field.val)
		}
	}
	docSize := len(buf) - docOff
	endian.PutUint32(buf[docOff+docSizeOff:], uint32(docSize))
	return buf, nil
}

func appendBinaryValElem(buf []byte, docOff, valEntryOff int, val any) ([]byte, error) {
	var typeCode TpCode
	var err error
	elemDocOff := len(buf)
	typeCode, buf, err = appendBinaryJSON(buf, val)
	if err != nil {
		return nil, moerr.NewInvalidArgNoCtx("invalid json value", val)
	}
	if typeCode == TpCodeLiteral {
		litCode := buf[elemDocOff]
		buf = buf[:elemDocOff]
		buf[valEntryOff] = TpCodeLiteral
		buf[valEntryOff+1] = litCode
		return buf, nil
	}
	buf[valEntryOff] = typeCode
	valOff := elemDocOff - docOff
	endian.PutUint32(buf[valEntryOff+1:], uint32(valOff))
	return buf, nil
}

func appendZero(buf []byte, length int) []byte {
	var tmp [8]byte
	rem := length % 8
	loop := length / 8
	for i := 0; i < loop; i++ {
		buf = append(buf, tmp[:]...)
	}
	for i := 0; i < rem; i++ {
		buf = append(buf, 0)
	}
	return buf
}
