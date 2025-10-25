package rsql

import (
	"fmt"
	"strconv"
	"strings"
)

// Precidence enumerations
const (
	_ = iota
	PrecedenceLowest
	PrecedenceAND
	PrecedenceOR
	PrecedenceCompare
	PrecedenceHighest
)

var precidences = map[TokenType]int{
	TokEQ:     PrecedenceCompare,
	TokNEQ:    PrecedenceCompare,
	TokLT:     PrecedenceCompare,
	TokLE:     PrecedenceCompare,
	TokGT:     PrecedenceCompare,
	TokGE:     PrecedenceCompare,
	TokLIKE:   PrecedenceCompare,
	TokExtend: PrecedenceCompare,
	TokOR:     PrecedenceOR,
	TokAND:    PrecedenceAND,
}

type (
	prefixParseFn func() Expression
	infixParseFn  func(expression Expression) Expression
)

// Parser reads lexed values and builds an AST
type Parser struct {
	l      *Lexer
	errors []string

	curToken  Token
	peekToken Token

	prefixParseFns map[TokenType]prefixParseFn
	infixParseFns  map[TokenType]infixParseFn
}

// NewParser returns a parser for a given lexer
func NewParser(l *Lexer) *Parser {
	p := &Parser{l: l}

	p.prefixParseFns = make(map[TokenType]prefixParseFn)
	p.registerPrefix(TokIdent, p.parseIdentifier)
	p.registerPrefix(TokInteger, p.parseInteger)
	p.registerPrefix(TokFloat, p.parseFloat)
	p.registerPrefix(TokTRUE, p.parseBool)
	p.registerPrefix(TokFALSE, p.parseBool)
	p.registerPrefix(TokNULL, p.parseNull)
	p.registerPrefix(TokString, p.parseString)
	p.registerPrefix(TokLParen, p.parseGroupedExpression)
	p.registerPrefix(TokLBracket, p.parseArray)

	p.infixParseFns = make(map[TokenType]infixParseFn)
	p.registerInfix(TokEQ, p.parseInfixExpression)
	p.registerInfix(TokNEQ, p.parseInfixExpression)
	p.registerInfix(TokLT, p.parseInfixExpression)
	p.registerInfix(TokLE, p.parseInfixExpression)
	p.registerInfix(TokGT, p.parseInfixExpression)
	p.registerInfix(TokGE, p.parseInfixExpression)
	p.registerInfix(TokLIKE, p.parseInfixExpression)
	p.registerInfix(TokExtend, p.parseInfixExpression)
	p.registerInfix(TokAND, p.parseInfixExpression)
	p.registerInfix(TokOR, p.parseInfixExpression)

	p.nextToken()
	p.nextToken()

	return p
}

// DefaultParse sets up a default lex/parse and returns the program
func DefaultParse(in string) *Program {
	l := NewLexer(in)
	p := NewParser(l)
	return p.ParseProgram()
}

func (p *Parser) registerPrefix(tokenType TokenType, fn prefixParseFn) {
	p.prefixParseFns[tokenType] = fn
}

func (p *Parser) registerInfix(tokenType TokenType, fn infixParseFn) {
	p.infixParseFns[tokenType] = fn
}

// Errors returns a list of errors while parsing
func (p *Parser) Errors() []string {
	return p.errors
}

func (p *Parser) peekError(t TokenType) {
	msg := fmt.Sprintf("expected next token to be %s, got %s instad",
		t, p.peekToken.Type)
	p.errors = append(p.errors, msg)
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

func (p *Parser) curTokenIs(t TokenType) bool {
	return p.curToken.Type == t
}

func (p *Parser) peekTokenIs(t TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) expectPeek(t TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	}
	p.peekError(t)
	return false
}

func (p *Parser) peekPrecedence() int {
	if p, ok := precidences[p.peekToken.Type]; ok {
		return p
	}
	return PrecedenceLowest
}

func (p *Parser) curPrecedence() int {
	if p, ok := precidences[p.curToken.Type]; ok {
		return p
	}
	return PrecedenceLowest
}

// ParseProgram builds a program AST from lexer
func (p *Parser) ParseProgram() *Program {
	program := &Program{}
	program.Statements = []Statement{}

	for p.curToken.Type != TokEOF {
		stmt := p.parseStatement()
		if stmt != nil {
			program.Statements = append(program.Statements, stmt)
		}
		p.nextToken()
	}

	return program
}

func (p *Parser) parseStatement() Statement {
	switch p.curToken.Type {
	default:
		return p.parseExpressionStatement()
	}
}

func (p *Parser) parseExpressionStatement() *ExpressionStatement {
	stmt := &ExpressionStatement{Token: p.curToken}
	stmt.Expression = p.parseExpression(PrecedenceLowest)

	return stmt
}

func (p *Parser) parseExpression(precedence int) Expression {
	prefix := p.prefixParseFns[p.curToken.Type]
	if prefix == nil {
		msg := fmt.Sprintf("no prefix parse function for %s found", p.curToken.Type)
		p.errors = append(p.errors, msg)
		return nil
	}
	leftExp := prefix()

	for !p.peekTokenIs(TokEOF) && precedence < p.peekPrecedence() {
		infix := p.infixParseFns[p.peekToken.Type]
		if infix == nil {
			return leftExp
		}

		p.nextToken()

		leftExp = infix(leftExp)
	}

	return leftExp
}

func (p *Parser) parseIdentifier() Expression {
	return &Identifier{Token: p.curToken, Value: p.curToken.Literal}
}

func (p *Parser) parseInteger() Expression {
	lit := &Integer{Token: p.curToken}

	value, err := strconv.ParseInt(p.curToken.Literal, 0, 64)
	if err != nil {
		msg := fmt.Sprintf("could not parse %q as integer", p.curToken.Literal)
		p.errors = append(p.errors, msg)
		return nil
	}

	lit.Value = value
	return lit
}

func (p *Parser) parseFloat() Expression {
	lit := &Float{Token: p.curToken}

	value, err := strconv.ParseFloat(p.curToken.Literal, 64)
	if err != nil {
		msg := fmt.Sprintf("could not parse %q as float", p.curToken.Literal)
		p.errors = append(p.errors, msg)
		return nil
	}

	lit.Value = value
	return lit
}

func (p *Parser) parseBool() Expression {
	return &Bool{Token: p.curToken, Value: p.curTokenIs(TokTRUE)}
}

func (p *Parser) parseString() Expression {
	s := p.curToken.Literal
	s = strings.ReplaceAll(s, `\'`, `'`)
	s = strings.ReplaceAll(s, `\"`, `"`)

	return &String{Token: p.curToken, Value: s}
}

func (p *Parser) parseNull() Expression {
	return &Null{Token: p.curToken}
}

func (p *Parser) parseArray() Expression {
	array := &Array{Token: p.curToken}
	array.Elements = p.parseExpressionList(TokRBracket)
	return array
}

func (p *Parser) parseExpressionList(end TokenType) []Expression {
	var list []Expression

	if p.peekTokenIs(end) {
		p.nextToken()
		return list
	}

	p.nextToken()
	list = append(list, p.parseExpression(PrecedenceHighest))
	for p.peekTokenIs(TokOR) {
		p.nextToken()
		p.nextToken()
		list = append(list, p.parseExpression(PrecedenceHighest))
	}

	if !p.expectPeek(end) {
		return nil
	}

	return list
}

func (p *Parser) parseInfixExpression(left Expression) Expression {
	expression := &InfixExpression{
		Token:    p.curToken,
		Left:     left,
		Operator: p.curToken.Literal,
	}

	precidence := p.curPrecedence()
	p.nextToken()
	expression.Right = p.parseExpression(precidence)

	return expression
}

func (p *Parser) parseGroupedExpression() Expression {
	p.nextToken()

	exp := p.parseExpression(PrecedenceLowest)

	if !p.expectPeek(TokRParen) {
		return nil
	}

	return exp
}
