// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package genlib

import (
	"bytes"
	"math"
	"testing"

	"github.com/elastic/elastic-integration-corpus-generator-tool/pkg/genlib/config"
)

// Test that byte values respect the -128 to 127 range
func Test_ByteFieldRespectsBounds(t *testing.T) {
	fld := Field{
		Name: "test_byte",
		Type: FieldTypeByte,
	}

	template := []byte(`{"test_byte":{{.test_byte}}}`)
	cfg, err := config.LoadConfigFromYaml([]byte(""))
	if err != nil {
		t.Fatal(err)
	}

	nSpins := 10000
	g := makeGeneratorWithCustomTemplate(t, cfg, []Field{fld}, template, uint64(nSpins))

	for i := 0; i < nSpins; i++ {
		var buf bytes.Buffer
		if err := g.Emit(&buf); err != nil {
			t.Fatal(err)
		}

		m := unmarshalJSONT[int8](t, buf.Bytes())
		val := m["test_byte"]

		if val < math.MinInt8 || val > math.MaxInt8 {
			t.Errorf("Byte value %d is outside valid range [%d, %d]", val, math.MinInt8, math.MaxInt8)
		}
	}
}

// Test that short values respect the -32768 to 32767 range
func Test_ShortFieldRespectsBounds(t *testing.T) {
	fld := Field{
		Name: "test_short",
		Type: FieldTypeShort,
	}

	template := []byte(`{"test_short":{{.test_short}}}`)
	cfg, err := config.LoadConfigFromYaml([]byte(""))
	if err != nil {
		t.Fatal(err)
	}

	nSpins := 10000
	g := makeGeneratorWithCustomTemplate(t, cfg, []Field{fld}, template, uint64(nSpins))

	for i := 0; i < nSpins; i++ {
		var buf bytes.Buffer
		if err := g.Emit(&buf); err != nil {
			t.Fatal(err)
		}

		m := unmarshalJSONT[int16](t, buf.Bytes())
		val := m["test_short"]

		if val < math.MinInt16 || val > math.MaxInt16 {
			t.Errorf("Short value %d is outside valid range [%d, %d]", val, math.MinInt16, math.MaxInt16)
		}
	}
}

// Test that integer values respect the -2^31 to 2^31-1 range
func Test_IntegerFieldRespectsBounds(t *testing.T) {
	fld := Field{
		Name: "test_integer",
		Type: FieldTypeInteger,
	}

	template := []byte(`{"test_integer":{{.test_integer}}}`)
	cfg, err := config.LoadConfigFromYaml([]byte(""))
	if err != nil {
		t.Fatal(err)
	}

	nSpins := 10000
	g := makeGeneratorWithCustomTemplate(t, cfg, []Field{fld}, template, uint64(nSpins))

	for i := 0; i < nSpins; i++ {
		var buf bytes.Buffer
		if err := g.Emit(&buf); err != nil {
			t.Fatal(err)
		}

		m := unmarshalJSONT[int32](t, buf.Bytes())
		val := m["test_integer"]

		if val < math.MinInt32 || val > math.MaxInt32 {
			t.Errorf("Integer value %d is outside valid range [%d, %d]", val, math.MinInt32, math.MaxInt32)
		}
	}
}

// Test that configured ranges get clamped to type bounds
func Test_ByteFieldWithLargeRangeGetsClamped(t *testing.T) {
	fld := Field{
		Name: "test_byte",
		Type: FieldTypeByte,
	}

	template := []byte(`{"test_byte":{{.test_byte}}}`)
	// Try to configure a range larger than byte can support
	configYaml := []byte(`fields:
  - name: test_byte
    range:
      min: 0
      max: 1000`)

	cfg, err := config.LoadConfigFromYaml(configYaml)
	if err != nil {
		t.Fatal(err)
	}

	nSpins := 1000
	g := makeGeneratorWithCustomTemplate(t, cfg, []Field{fld}, template, uint64(nSpins))

	for i := 0; i < nSpins; i++ {
		var buf bytes.Buffer
		if err := g.Emit(&buf); err != nil {
			t.Fatal(err)
		}

		m := unmarshalJSONT[int8](t, buf.Bytes())
		val := m["test_byte"]

		// Value should be clamped to byte range
		if val < 0 || val > math.MaxInt8 {
			t.Errorf("Byte value %d exceeds clamped range [0, %d]", val, math.MaxInt8)
		}
	}
}

// Test that counter mode respects type bounds
func Test_ByteCounterRespectsBounds(t *testing.T) {
	fld := Field{
		Name: "test_byte_counter",
		Type: FieldTypeByte,
	}

	template := []byte(`{"test_byte_counter":{{.test_byte_counter}}}`)
	configYaml := []byte(`fields:
  - name: test_byte_counter
    counter: true`)

	cfg, err := config.LoadConfigFromYaml(configYaml)
	if err != nil {
		t.Fatal(err)
	}

	nSpins := 200
	g := makeGeneratorWithCustomTemplate(t, cfg, []Field{fld}, template, uint64(nSpins))

	var lastVal int8 = 0
	for i := 0; i < nSpins; i++ {
		var buf bytes.Buffer
		if err := g.Emit(&buf); err != nil {
			t.Fatal(err)
		}

		m := unmarshalJSONT[int8](t, buf.Bytes())
		val := m["test_byte_counter"]

		if val < math.MinInt8 || val > math.MaxInt8 {
			t.Errorf("Byte counter value %d is outside valid range [%d, %d]", val, math.MinInt8, math.MaxInt8)
		}

		// Counter should be monotonically increasing (or stay at max)
		if val < lastVal {
			t.Errorf("Counter decreased from %d to %d", lastVal, val)
		}

		// Once it hits the max, it should stay there or reset
		if lastVal == math.MaxInt8 && val != math.MaxInt8 && val != 0 {
			t.Logf("Counter hit max (%d) and reset to %d", math.MaxInt8, val)
		}

		lastVal = val
	}
}

// Test that fuzziness respects type bounds
func Test_ByteFuzzinessRespectsBounds(t *testing.T) {
	fld := Field{
		Name: "test_byte_fuzzy",
		Type: FieldTypeByte,
	}

	template := []byte(`{"test_byte_fuzzy":{{.test_byte_fuzzy}}}`)
	// Start near max and use fuzziness - should not exceed max
	configYaml := []byte(`fields:
  - name: test_byte_fuzzy
    fuzziness: 0.5
    range:
      min: 100
      max: 127`)

	cfg, err := config.LoadConfigFromYaml(configYaml)
	if err != nil {
		t.Fatal(err)
	}

	nSpins := 1000
	g := makeGeneratorWithCustomTemplate(t, cfg, []Field{fld}, template, uint64(nSpins))

	for i := 0; i < nSpins; i++ {
		var buf bytes.Buffer
		if err := g.Emit(&buf); err != nil {
			t.Fatal(err)
		}

		m := unmarshalJSONT[int8](t, buf.Bytes())
		val := m["test_byte_fuzzy"]

		if val < 100 || val > math.MaxInt8 {
			t.Errorf("Byte fuzzy value %d is outside valid range [100, %d]", val, math.MaxInt8)
		}
	}
}
