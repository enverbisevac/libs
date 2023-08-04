package rsql

import (
	"bytes"
	"strings"
)

// Node is the smallest unit of ast
type Node interface {
	TokenLiteral() string
	String() string
}

// Statement is a executable tree
type Statement interface {
	Node
	statementNode()
}

// Expression is a portion of tree
type Expression interface {
	Node
	expressionNode()
}

// Identifier is a variable name
type Identifier struct {
	Token Token
	Value string
}

func (i *Identifier) expressionNode() {}

// TokenLiteral returns the literal value of a token
func (i *Identifier) TokenLiteral() string { return i.Token.Literal }

// String returns a string representation of value
func (i *Identifier) String() string { return i.Value }

// Integer is a numeric value
type Integer struct {
	Token Token
	Value int64
}

func (i *Integer) expressionNode() {}

// TokenLiteral returns the literal value of a token
func (i *Integer) TokenLiteral() string { return i.Token.Literal }

// String returns a string representation of value
func (i *Integer) String() string { return i.Token.Literal }

// Float is a floating point value
type Float struct {
	Token Token
	Value float64
}

func (i *Float) expressionNode() {}

// TokenLiteral returns the literal value of a token
func (i *Float) TokenLiteral() string { return i.Token.Literal }

// String returns a string representation of value
func (i *Float) String() string { return i.Token.Literal }

// Bool is a boolean value
type Bool struct {
	Token Token
	Value bool
}

func (i *Bool) expressionNode() {}

// TokenLiteral returns the literal value of a token
func (i *Bool) TokenLiteral() string { return i.Token.Literal }

// String returns a string representation of value
func (i *Bool) String() string { return i.Token.Literal }

// Null is an empty value
type Null struct {
	Token Token
}

func (i *Null) expressionNode() {}

// TokenLiteral returns the literal value of a token
func (i *Null) TokenLiteral() string { return i.Token.Literal }

// String returns a string representation of value
func (i *Null) String() string { return i.Token.Literal }

// String is an array of codepoints
type String struct {
	Token Token
	Value string
}

func (i *String) expressionNode() {}

// TokenLiteral returns the literal value of a token
func (i *String) TokenLiteral() string { return i.Token.Literal }

// String returns a string representation of value
func (i *String) String() string {
	var out bytes.Buffer

	out.WriteRune('"')
	out.WriteString(i.Value)
	out.WriteRune('"')

	return out.String()
}

// Array is an array of tokens
type Array struct {
	Token    Token
	Elements []Expression
}

func (a *Array) expressionNode() {}

// TokenLiteral returns the literal value of a token
func (a *Array) TokenLiteral() string { return a.Token.Literal }

// String returns a string representation of value
func (a *Array) String() string {
	var out bytes.Buffer

	var elements []string
	for _, el := range a.Elements {
		elements = append(elements, el.String())
	}

	out.WriteRune('(')
	out.WriteString(strings.Join(elements, ","))
	out.WriteRune(')')

	return out.String()
}

// Program is a collection of statements
type Program struct {
	Statements []Statement
}

func (p *Program) expressionNode() {}

// TokenLiteral returns the literal value of a token
func (p *Program) TokenLiteral() string {
	if len(p.Statements) > 0 {
		return p.Statements[0].TokenLiteral()
	}
	return ""
}

// String returns a string representation of value
func (p *Program) String() string {
	var out bytes.Buffer

	for _, s := range p.Statements {
		out.WriteString(s.String())
	}

	return out.String()
}

// ExpressionStatement is a collection of expressions
type ExpressionStatement struct {
	Token      Token
	Expression Expression
}

func (ExpressionStatement) statementNode() {}

// TokenLiteral returns the literal value of a token
func (e ExpressionStatement) TokenLiteral() string { return e.Token.Literal }

// String returns a string representation of value
func (e ExpressionStatement) String() string {
	if e.Expression != nil {
		return e.Expression.String()
	}

	return ""
}

// PrefixExpression is an expression with a preceeding operator
type PrefixExpression struct {
	Token    Token
	Operator string
	Right    Expression
}

func (p *PrefixExpression) expressionNode() {}

// TokenLiteral returns the literal value of a token
func (p *PrefixExpression) TokenLiteral() string { return p.Token.Literal }

// String returns a string representation of value
func (p *PrefixExpression) String() string {
	var out bytes.Buffer

	out.WriteRune('(')
	out.WriteString(p.Operator)
	out.WriteString(p.Right.String())
	out.WriteRune(')')

	return out.String()
}

// InfixExpression is two expressions with a infix operator
type InfixExpression struct {
	Token    Token
	Left     Expression
	Operator string
	Right    Expression
}

func (i *InfixExpression) expressionNode() {}

// TokenLiteral returns the literal value of a token
func (i *InfixExpression) TokenLiteral() string { return i.Token.Literal }

// String returns a string representation of value
func (i *InfixExpression) String() string {
	var out bytes.Buffer

	out.WriteRune('(')
	if i.Left != nil {
		out.WriteString(i.Left.String())
	} else {
		out.WriteString("nil")
	}
	out.WriteString(i.Operator)
	if i.Right != nil {
		out.WriteString(i.Right.String())
	} else {
		out.WriteString("nil")
	}
	out.WriteRune(')')

	return out.String()
}
