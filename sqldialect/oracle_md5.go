package sqldialect

// oracleMaxVarcharBytes is the most a VARCHAR2 holds in SQL with
// MAX_STRING_SIZE=STANDARD, the default. EXTENDED raises it, and a text past
// this many bytes is then hashed as a CLOB too, to the same digest.
const oracleMaxVarcharBytes = 4000

// oracleDbmsCryptoHashMd5 is DBMS_CRYPTO.HASH_MD5. SQL cannot read a package
// constant, so the call passes its value.
const oracleDbmsCryptoHashMd5 = 2

// OracleMd5OfLongText returns the MD5 digest of text as a RAW(16), the value
// STANDARD_HASH(text, 'MD5') returns, for a text that may not fit a VARCHAR2.
//
// STANDARD_HASH takes no LOB, and joining VARCHAR2 values fails with ORA-01489
// once the result passes 4000 bytes, which a wide row or a few long values
// reach. When text is a ConcatWs, a row whose values add up to more than that
// is joined as a CLOB and hashed with DBMS_CRYPTO.HASH, which hashes a CLOB as
// its AL32UTF8 bytes, the bytes STANDARD_HASH hashes in an AL32UTF8 database.
// A shorter row still goes through STANDARD_HASH. Either way the digest is the
// MD5 of the text's UTF-8 bytes, the one every other engine computes for the
// same values, so a row hashes the same whichever branch it took. Any other
// text is hashed by STANDARD_HASH alone.
//
// The statement names DBMS_CRYPTO whatever the row, and Oracle grants EXECUTE
// on it to no one by default, so the querying user needs
//
//	GRANT EXECUTE ON SYS.DBMS_CRYPTO TO <user>
//
// or the statement fails to parse with ORA-00904 "DBMS_CRYPTO"."HASH": invalid
// identifier. A caller that may lack the grant keeps STANDARD_HASH for texts it
// knows stay under 4000 bytes.
func OracleMd5OfLongText(text Expr) Expr {
	concat, ok := text.(*concatWsExpr)
	if !ok || len(concat.exprs) < 2 {
		return WrapSql("STANDARD_HASH(%s, 'MD5')", text)
	}
	oracle := NewOracleDialect()

	// The byte length of the joined text, summed from its values: building the
	// text as a VARCHAR2 to measure it is what fails. Oracle's || reads a NULL
	// as an empty string, so a NULL value adds nothing.
	lengths := make([]Expr, 0, len(concat.exprs)+1)
	for _, e := range concat.exprs {
		lengths = append(lengths, WrapSql("NVL(LENGTHB(%s), 0)", e))
	}
	lengths = append(lengths, Int64(int64(len(concat.separator)*(len(concat.exprs)-1))))
	byteLength := lengths[0]
	for _, l := range lengths[1:] {
		byteLength = WrapSql("%s + %s", byteLength, l)
	}

	// Every separator is a CLOB, so no two VARCHAR2 values meet without one
	// between them. A CLOB as the first operand alone is not enough: Oracle
	// still joins a long enough run of the VARCHAR2 operands after it as a
	// VARCHAR2 and fails with ORA-01489 on a wide row.
	clobSeparator := Fn("TO_CLOB", String(concat.separator))
	asClob := concat.exprs[0]
	for _, e := range concat.exprs[1:] {
		asClob = WrapSql("%s || %s || %s", asClob, clobSeparator, e)
	}

	return WrapSql("CASE WHEN %s <= %s THEN STANDARD_HASH(%s, 'MD5') ELSE DBMS_CRYPTO.HASH(%s, %s) END",
		byteLength,
		Int64(oracleMaxVarcharBytes),
		oracle.ConcatWithSeparator(concat.separator, concat.exprs...),
		asClob,
		Int64(oracleDbmsCryptoHashMd5),
	)
}
