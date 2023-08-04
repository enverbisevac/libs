package rsql

import (
	"unicode"
	"unicode/utf8"
)

// Lexer reads tokens from input
type Lexer struct {
	input        string
	position     int
	readPosition int
	rune         rune
}

// NewLexer returns a new lexing generator
func NewLexer(in string) *Lexer {
	l := &Lexer{input: in}
	l.readRune()
	return l
}

// NextToken returns the next token from lexer
func (l *Lexer) NextToken() Token {
	var tok Token

	l.skipSpace()

	switch l.rune {
	case '-':
		l.readRune()
		if isNumber(l.rune) {
			var isFloat bool
			tok.Literal, isFloat = l.readNumber()
			if isFloat {
				tok.Type = TokFloat
			} else {
				tok.Type = TokInteger
			}

		} else if isLetter(l.rune) {
			tok.Literal = l.readIdentifier()
			tok.Type = lookupIdent(tok.Literal)

		} else {
			tok = newToken(TokIllegal, l.rune)
			return tok
		}

		tok.Literal = "-" + tok.Literal
		return tok
	case '=':
		r := l.peekRune()
		if r == '=' {
			r := l.rune
			l.readRune()
			tok.Type, tok.Literal = TokEQ, string(r)+string(l.rune)
		} else if isLetter(r) {
			tok = l.readFIQL()

			return tok
		} else {
			tok = newToken(TokIllegal, l.rune)
		}
	case ';':
		tok = newToken(TokAND, l.rune)
	case ',':
		tok = newToken(TokOR, l.rune)
	case ')':
		tok = newToken(TokRParen, l.rune)
	case '(':
		tok = newToken(TokLParen, l.rune)
	case ']':
		tok = newToken(TokRBracket, l.rune)
	case '[':
		tok = newToken(TokLBracket, l.rune)
	case '~':
		tok = newToken(TokLIKE, l.rune)
	case '!':
		if l.peekRune() == '=' {
			r := l.rune
			l.readRune()
			tok.Type, tok.Literal = TokNEQ, string(r)+string(l.rune)
		} else if l.peekRune() == '~' {
			r := l.rune
			l.readRune()
			tok.Type, tok.Literal = TokNLIKE, string(r)+string(l.rune)
		} else {
			tok = newToken(TokIllegal, l.rune)
			return tok
		}
	case '<':
		if l.peekRune() == '=' {
			r := l.rune
			l.readRune()
			tok.Type, tok.Literal = TokLE, string(r)+string(l.rune)
		} else {
			tok = newToken(TokLT, l.rune)
		}
	case '>':
		if l.peekRune() == '=' {
			r := l.rune
			l.readRune()
			tok.Type, tok.Literal = TokGE, string(r)+string(l.rune)
		} else {
			tok = newToken(TokGT, l.rune)
		}
	case '"', '\'':
		tok.Type = TokString
		tok.Literal = l.readString(l.rune)
	case 0:
		tok.Type, tok.Literal = TokEOF, ""
	default:
		if isNumber(l.rune) {
			var isFloat bool
			tok.Literal, isFloat = l.readNumber()
			if isFloat {
				tok.Type = TokFloat
			} else {
				tok.Type = TokInteger
			}

		} else if isLetter(l.rune) {
			tok.Literal = l.readIdentifier()
			tok.Type = lookupIdent(tok.Literal)

		} else {
			tok = newToken(TokIllegal, l.rune)
			return tok
		}

		return tok
	}

	l.readRune()
	return tok
}

func (l *Lexer) readRune() {
	var size int
	if l.readPosition >= len(l.input) {
		l.rune = 0
	} else {
		l.rune, size = utf8.DecodeRuneInString(l.input[l.readPosition:])
	}

	l.position = l.readPosition
	l.readPosition += size
}
func (l *Lexer) peekRune() rune {
	if l.readPosition >= len(l.input) {
		return 0
	}
	r, _ := utf8.DecodeRuneInString(l.input[l.readPosition:])
	return r
}

func (l *Lexer) skipSpace() {
	for unicode.IsSpace(l.rune) {
		l.readRune()
	}
}

func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.rune) {
		l.readRune()
	}

	return l.input[position:l.position]
}
func (l *Lexer) readNumber() (string, bool) {
	isFloat := false

	position := l.position
	for isNumber(l.rune) {
		if l.rune == '.' {
			isFloat = true
		}

		l.readRune()
	}

	return l.input[position:l.position], isFloat
}
func (l *Lexer) readString(st rune) string {
	position := l.position + 1
	escape := false
	for {
		l.readRune()

		if l.rune == '\\' {
			escape = true

			continue
		}
		if escape {
			escape = false
			continue
		}
		if l.rune == st || l.rune == 0 {
			break
		}
	}

	return l.input[position:l.position]

}
func (l *Lexer) readFIQL() Token {
	l.readRune()
	s := l.readIdentifier()
	if l.rune != '=' {
		return Token{TokIllegal, "=" + s}
	}
	l.readRune()

	switch s {
	case "eq":
		return Token{TokEQ, "=" + s + "="}
	case "neq":
		return Token{TokNEQ, "=" + s + "="}
	case "gt":
		return Token{TokGT, "=" + s + "="}
	case "ge":
		return Token{TokGE, "=" + s + "="}
	case "lt":
		return Token{TokLT, "=" + s + "="}
	case "le":
		return Token{TokLE, "=" + s + "="}
	default:
		return Token{TokExtend, "=" + s + "="}
	}
}

func isLetter(r rune) bool {
	if unicode.IsSpace(r) {
		return false
	}
	switch r {
	case '"', '\'', '(', ')', ';', ',', '=', '!', '~', '<', '>', '[', ']':
		return false
	}
	if '0' < r && r < '9' || r == '.' {
		return false
	}

	return unicode.IsPrint(r)
}
func isNumber(r rune) bool {
	if '0' <= r && r <= '9' || r == '.' {
		return true
	}
	return false
}
