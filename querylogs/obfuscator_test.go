package querylogs

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ObfuscatorSuite struct {
	suite.Suite
}

func TestObfuscatorSuite(t *testing.T) {
	suite.Run(t, new(ObfuscatorSuite))
}

func (s *ObfuscatorSuite) TestObfuscatorNone() {
	obfuscator, err := NewQueryObfuscator(ObfuscationNone)
	s.Require().NoError(err)

	s.Equal(ObfuscationNone, obfuscator.Mode(), "Expected ObfuscationNone")

	sql := "SELECT * FROM users WHERE email = 'user@example.com' AND age > 25"
	result := obfuscator.Obfuscate(sql)
	s.Equal(sql, result, "ObfuscationNone should not modify SQL")
}

func (s *ObfuscatorSuite) TestObfuscatorEmptyString() {
	obfuscator, err := NewQueryObfuscator(ObfuscationRedactLiterals)
	s.Require().NoError(err)

	tests := []struct {
		name  string
		input string
	}{
		{"empty string", ""},
		{"whitespace only", "   "},
		{"tabs and spaces", "\t  \n  "},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			result := obfuscator.Obfuscate(tt.input)
			s.Equal(tt.input, result, "Empty/whitespace strings should be returned unchanged")
		})
	}
}

func (s *ObfuscatorSuite) TestObfuscatorRedactLiterals() {
	obfuscator, err := NewQueryObfuscator(ObfuscationRedactLiterals)
	s.Require().NoError(err)

	s.Equal(ObfuscationRedactLiterals, obfuscator.Mode(), "Expected ObfuscationRedactLiterals")

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "string literals",
			input:    "SELECT * FROM users WHERE email = 'user@example.com'",
			expected: "SELECT * FROM users WHERE email = ?",
		},
		{
			name:     "numeric literals replaced",
			input:    "SELECT * FROM users WHERE age > 25 AND score = 98.5",
			expected: "SELECT * FROM users WHERE age > ? AND score = ?",
		},
		{
			name:     "identifiers with digits preserved",
			input:    "SELECT col1, col2 FROM table1 WHERE id = 123",
			expected: "SELECT col1, col2 FROM table1 WHERE id = ?",
		},
		{
			name:     "mixed literals",
			input:    "INSERT INTO logs VALUES ('error', 123, 45.67, 'message')",
			expected: "INSERT INTO logs VALUES (?, ?, ?, ?)",
		},
		{
			name:     "complex query",
			input:    "SELECT name FROM users WHERE created_at > '2023-10-01' AND status = 'active'",
			expected: "SELECT name FROM users WHERE created_at > ? AND status = ?",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			result := obfuscator.Obfuscate(tt.input)
			s.Equal(tt.expected, result, "Obfuscation mismatch for %s", tt.name)
		})
	}
}

func (s *ObfuscatorSuite) TestSqlFormattingPreserved() {
	obfuscator, err := NewQueryObfuscator(ObfuscationRedactLiterals)
	s.Require().NoError(err)

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name: "multiline query with indentation",
			input: `SELECT
    user_id,
    email,
    created_at
FROM users
WHERE
    status = 'active'
    AND age > 18
    AND country = 'US'`,
			expected: `SELECT
    user_id,
    email,
    created_at
FROM users
WHERE
    status = ?
    AND age > ?
    AND country = ?`,
		},
		{
			name:     "query with tabs",
			input:    "SELECT\n\tname,\n\tid\nFROM\n\tusers\nWHERE\n\tid = 123",
			expected: "SELECT\n\tname,\n\tid\nFROM\n\tusers\nWHERE\n\tid = ?",
		},
		{
			name: "query with mixed whitespace",
			input: `SELECT  name,   email
FROM    users
WHERE   created_at  >  '2023-01-01'
  AND   status   =   'active'`,
			expected: `SELECT  name,   email
FROM    users
WHERE   created_at  >  ?
  AND   status   =   ?`,
		},
		{
			name:     "single line with extra spaces",
			input:    "SELECT  *  FROM  users  WHERE  id  =  42",
			expected: "SELECT  *  FROM  users  WHERE  id  =  ?",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			result := obfuscator.Obfuscate(tt.input)
			s.Equal(tt.expected, result, "SQL formatting should be preserved during obfuscation")
		})
	}
}

func (s *ObfuscatorSuite) TestPreserveLiteralsMatchingDates() {
	// Pattern to match ISO 8601 date format: YYYY-MM-DD
	datePattern := `^\d{4}-\d{2}-\d{2}$`
	// Pattern to match ISO 8601 timestamp: YYYY-MM-DD HH:MM:SS
	timestampPattern := `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`

	obfuscator, err := NewQueryObfuscator(
		ObfuscationRedactLiterals,
		WithPreserveLiteralsMatching([]string{datePattern, timestampPattern}),
	)
	s.Require().NoError(err)

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "preserve date literal",
			input:    "SELECT * FROM users WHERE created_at > '2023-10-01'",
			expected: "SELECT * FROM users WHERE created_at > '2023-10-01'",
		},
		{
			name:     "preserve timestamp literal",
			input:    "SELECT * FROM events WHERE occurred_at = '2023-10-01 14:30:00'",
			expected: "SELECT * FROM events WHERE occurred_at = '2023-10-01 14:30:00'",
		},
		{
			name:     "preserve date but obfuscate other strings",
			input:    "SELECT * FROM users WHERE created_at > '2023-10-01' AND email = 'user@example.com'",
			expected: "SELECT * FROM users WHERE created_at > '2023-10-01' AND email = ?",
		},
		{
			name:     "obfuscate invalid date format",
			input:    "SELECT * FROM users WHERE created_at > '10-01-2023'",
			expected: "SELECT * FROM users WHERE created_at > ?",
		},
		{
			name:     "preserve multiple dates in query",
			input:    "SELECT * FROM events WHERE start_date = '2023-10-01' AND end_date = '2023-10-31'",
			expected: "SELECT * FROM events WHERE start_date = '2023-10-01' AND end_date = '2023-10-31'",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			result := obfuscator.Obfuscate(tt.input)
			s.Equal(tt.expected, result, "Date preservation mismatch for %s", tt.name)
		})
	}
}

func (s *ObfuscatorSuite) TestPreserveLiteralsMatchingMultipleTypes() {
	// Patterns for dates, UUIDs, and booleans
	patterns := []string{
		`^\d{4}-\d{2}-\d{2}$`, // Date: YYYY-MM-DD
		`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`, // UUID
		`^(true|false)$`, // Boolean
	}

	obfuscator, err := NewQueryObfuscator(
		ObfuscationRedactLiterals,
		WithPreserveLiteralsMatching(patterns),
	)
	s.Require().NoError(err)

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "preserve UUID",
			input:    "SELECT * FROM users WHERE id = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'",
			expected: "SELECT * FROM users WHERE id = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'",
		},
		{
			name:     "preserve date and UUID",
			input:    "SELECT * FROM logs WHERE user_id = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890' AND date = '2023-10-01'",
			expected: "SELECT * FROM logs WHERE user_id = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890' AND date = '2023-10-01'",
		},
		{
			name:     "obfuscate non-matching strings",
			input:    "SELECT * FROM users WHERE email = 'user@example.com' AND name = 'John Doe'",
			expected: "SELECT * FROM users WHERE email = ? AND name = ?",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			result := obfuscator.Obfuscate(tt.input)
			s.Equal(tt.expected, result, "Pattern matching mismatch for %s", tt.name)
		})
	}
}

func (s *ObfuscatorSuite) TestPreserveLiteralsMatchingEmpty() {
	// No patterns means all literals should be replaced
	obfuscator, err := NewQueryObfuscator(
		ObfuscationRedactLiterals,
		WithPreserveLiteralsMatching([]string{}),
	)
	s.Require().NoError(err)

	sql := "SELECT * FROM users WHERE created_at > '2023-10-01' AND email = 'user@example.com'"
	result := obfuscator.Obfuscate(sql)
	expected := "SELECT * FROM users WHERE created_at > ? AND email = ?"
	s.Equal(expected, result, "Empty pattern list should replace all literals")
}

func (s *ObfuscatorSuite) TestPreserveLiteralsMatchingInvalidRegex() {
	// Invalid regex pattern should return error
	invalidPatterns := []string{
		`[invalid(`, // Unclosed bracket
	}

	obfuscator, err := NewQueryObfuscator(
		ObfuscationRedactLiterals,
		WithPreserveLiteralsMatching(invalidPatterns),
	)

	s.Error(err)
	s.Nil(obfuscator)
	s.Contains(err.Error(), "failed to compile preserve literal patterns")
}

func (s *ObfuscatorSuite) TestObfuscatorWithPreserveNumbers() {
	obfuscator, err := NewQueryObfuscator(
		ObfuscationRedactLiterals,
		WithPreserveNumbers(true),
	)
	s.Require().NoError(err)

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "preserve integer literals",
			input:    "SELECT * FROM users WHERE age > 25 AND score = 100",
			expected: "SELECT * FROM users WHERE age > 25 AND score = 100",
		},
		{
			name:     "preserve decimal literals",
			input:    "SELECT * FROM products WHERE price = 19.99 AND discount = 0.15",
			expected: "SELECT * FROM products WHERE price = 19.99 AND discount = 0.15",
		},
		{
			name:     "preserve numbers but replace strings",
			input:    "SELECT * FROM users WHERE age = 30 AND name = 'John'",
			expected: "SELECT * FROM users WHERE age = 30 AND name = ?",
		},
		{
			name:     "identifiers with digits always preserved",
			input:    "SELECT col1, table2.col3 FROM database4.schema5.table6 WHERE id = 123",
			expected: "SELECT col1, table2.col3 FROM database4.schema5.table6 WHERE id = 123",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			result := obfuscator.Obfuscate(tt.input)
			s.Equal(tt.expected, result, "Number preservation mismatch for %s", tt.name)
		})
	}
}

func (s *ObfuscatorSuite) TestObfuscatorWithPreserveNumbersDisabled() {
	obfuscator, err := NewQueryObfuscator(
		ObfuscationRedactLiterals,
		WithPreserveNumbers(false),
	)
	s.Require().NoError(err)

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "replace numeric literals",
			input:    "SELECT * FROM users WHERE age > 25 AND score = 98.5",
			expected: "SELECT * FROM users WHERE age > ? AND score = ?",
		},
		{
			name:     "identifiers with digits still preserved",
			input:    "SELECT col1, col2 FROM table1 WHERE id = 123",
			expected: "SELECT col1, col2 FROM table1 WHERE id = ?",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			result := obfuscator.Obfuscate(tt.input)
			s.Equal(tt.expected, result, "Number replacement mismatch for %s", tt.name)
		})
	}
}
