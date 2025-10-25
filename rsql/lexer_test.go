package rsql

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestReservedToken(t *testing.T) {
	input := `( ) ; , == != ~ < > <= >= [ ]`
	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TokLParen, "("},
		{TokRParen, ")"},
		{TokAND, ";"},
		{TokOR, ","},
		{TokEQ, "=="},
		{TokNEQ, "!="},
		{TokLIKE, "~"},
		{TokLT, "<"},
		{TokGT, ">"},
		{TokLE, "<="},
		{TokGE, ">="},
		{TokLBracket, "["},
		{TokRBracket, "]"},
	}

	Convey("Reserved Tokens", t, func() {
		l := NewLexer(input)

		for _, tt := range tests {
			tok := l.NextToken()
			So(tt.expectedType, ShouldEqual, tok.Type)
			So(tt.expectedLiteral, ShouldEqual, tok.Literal)
		}
	})
}

func TestNextToken(t *testing.T) {
	input := `director=='name\'s';actor=eq="name's";Year=le=2000,Year>=2010;(one <= -1.0, two != true),three=in=(1,2,3);three=out=(4,5,6)`
	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TokIdent, `director`},
		{TokEQ, `==`},
		{TokString, `name\'s`},
		{TokAND, `;`},
		{TokIdent, `actor`},
		{TokEQ, `=eq=`},
		{TokString, `name's`},
		{TokAND, `;`},
		{TokIdent, "Year"},
		{TokLE, "=le="},
		{TokInteger, "2000"},
		{TokOR, ","},
		{TokIdent, "Year"},
		{TokGE, ">="},
		{TokInteger, "2010"},
		{TokAND, ";"},
		{TokLParen, "("},
		{TokIdent, "one"},
		{TokLE, "<="},
		{TokFloat, "-1.0"},
		{TokOR, ","},
		{TokIdent, "two"},
		{TokNEQ, "!="},
		{TokTRUE, "true"},
		{TokRParen, ")"},
		{TokOR, ","},
		{TokIdent, "three"},
		{TokExtend, "=in="},
		{TokLParen, "("},
		{TokInteger, "1"},
		{TokOR, ","},
		{TokInteger, "2"},
		{TokOR, ","},
		{TokInteger, "3"},
		{TokRParen, ")"},
		{TokAND, ";"},
		{TokIdent, "three"},
		{TokExtend, "=out="},
		{TokLParen, "("},
		{TokInteger, "4"},
		{TokOR, ","},
		{TokInteger, "5"},
		{TokOR, ","},
		{TokInteger, "6"},
		{TokRParen, ")"},
	}

	Convey("Next Token Parsing", t, func() {
		l := NewLexer(input)

		c := 0
		for _, tt := range tests {
			c++
			tok := l.NextToken()

			So(tt.expectedType, ShouldEqual, tok.Type)
			So(tt.expectedLiteral, ShouldEqual, tok.Literal)

		}
		So(c, ShouldEqual, len(tests))
	})
}
