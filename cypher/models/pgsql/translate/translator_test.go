package translate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeCypherStringLiteral(t *testing.T) {
	t.Parallel()

	type expected struct {
		value       string
		errContains string
	}
	type testData struct {
		name     string
		raw      string
		expected expected
	}

	tt := []testData{
		{name: "success_-_empty_single_quoted", raw: `''`, expected: expected{value: ``}},
		{name: "success_-_empty_double_quoted", raw: `""`, expected: expected{value: ``}},
		{name: "success_-_single_character", raw: `'a'`, expected: expected{value: `a`}},
		{name: "success_-_plain_ascii", raw: `'hello world'`, expected: expected{value: `hello world`}},
		{name: "success_-_double_quoted_plain", raw: `"hello"`, expected: expected{value: `hello`}},

		{name: "success_-_escaped_backslash", raw: `'a\\b'`, expected: expected{value: `a\b`}},
		{name: "success_-_leading_backslash", raw: `'\\foo'`, expected: expected{value: `\foo`}},
		{name: "success_-_trailing_backslash", raw: `'foo\\'`, expected: expected{value: `foo\`}},
		{name: "success_-_only_backslash", raw: `'\\'`, expected: expected{value: `\`}},
		{name: "success_-_two_consecutive_backslashes", raw: `'\\\\'`, expected: expected{value: `\\`}},

		{name: "success_-_escaped_single_quote_in_single_quoted", raw: `'O\'Brien'`, expected: expected{value: `O'Brien`}},
		{name: "success_-_escaped_double_quote_in_double_quoted", raw: `"say \"hi\""`, expected: expected{value: `say "hi"`}},
		{name: "success_-_escaped_double_quote_in_single_quoted", raw: `'a\"b'`, expected: expected{value: `a"b`}},
		{name: "success_-_escaped_single_quote_in_double_quoted", raw: `"a\'b"`, expected: expected{value: `a'b`}},

		{name: "success_-_newline_escape", raw: `'a\nb'`, expected: expected{value: "a\nb"}},
		{name: "success_-_carriage_return_escape", raw: `'a\rb'`, expected: expected{value: "a\rb"}},
		{name: "success_-_tab_escape", raw: `'a\tb'`, expected: expected{value: "a\tb"}},
		{name: "success_-_backspace_escape", raw: `'a\bb'`, expected: expected{value: "a\bb"}},
		{name: "success_-_form_feed_escape", raw: `'a\fb'`, expected: expected{value: "a\fb"}},

		{name: "success_-_uppercase_newline_escape", raw: `'a\Nb'`, expected: expected{value: "a\nb"}},
		{name: "success_-_uppercase_tab_escape", raw: `'a\Tb'`, expected: expected{value: "a\tb"}},

		{name: "success_-_mssql_style_domain_object_id", raw: `'TEST\\PS1-PSV$@A-1-2-34'`, expected: expected{value: `TEST\PS1-PSV$@A-1-2-34`}},
		{name: "success_-_windows_path", raw: `'C:\\Users\\Admin'`, expected: expected{value: `C:\Users\Admin`}},
		{name: "success_-_mixed_escapes", raw: `'a\\b\nc\td'`, expected: expected{value: "a\\b\nc\td"}},

		{name: "success_-_raw_utf8_bmp_codepoint", raw: `'café'`, expected: expected{value: "café"}},
		{name: "success_-_raw_utf8_supplementary_codepoint", raw: `'😀'`, expected: expected{value: "😀"}},
		{name: "success_-_raw_utf8_double_quoted", raw: `"日本語"`, expected: expected{value: "日本語"}},

		{name: "error_-_empty_input", raw: ``, expected: expected{errContains: "invalid cypher string literal"}},
		{name: "error_-_single_quote_only", raw: `'`, expected: expected{errContains: "invalid cypher string literal"}},
		{name: "error_-_no_surrounding_quotes", raw: `foo`, expected: expected{errContains: "missing or mismatched surrounding quotes"}},
		{name: "error_-_unmatched_quote_styles", raw: `'foo"`, expected: expected{errContains: "missing or mismatched surrounding quotes"}},
		{name: "error_-_only_open_quote_with_text", raw: `'foo`, expected: expected{errContains: "missing or mismatched surrounding quotes"}},
		{name: "error_-_only_close_quote_with_text", raw: `foo'`, expected: expected{errContains: "missing or mismatched surrounding quotes"}},
		{name: "error_-_backtick_quoted", raw: "`foo`", expected: expected{errContains: "missing or mismatched surrounding quotes"}},
		{name: "error_-_dangling_backslash", raw: `'foo\'`, expected: expected{errContains: "dangling escape"}},
		{name: "error_-_invalid_escape_letter", raw: `'\x'`, expected: expected{errContains: `invalid escape \x`}},
		{name: "error_-_invalid_escape_digit", raw: `'\1'`, expected: expected{errContains: `invalid escape \1`}},
		{name: "error_-_bare_backslash_before_letter", raw: `'a\Pb'`, expected: expected{errContains: `invalid escape \P`}},
		{name: "error_-_lowercase_unicode_escape_unsupported", raw: `'\u0041'`, expected: expected{errContains: `invalid escape \u`}},
		{name: "error_-_uppercase_unicode_escape_unsupported", raw: `'\U0001F600'`, expected: expected{errContains: `invalid escape \U`}},
	}

	for _, testCase := range tt {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got, err := decodeCypherStringLiteral(testCase.raw); testCase.expected.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), testCase.expected.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, testCase.expected.value, got)
			}
		})
	}
}
