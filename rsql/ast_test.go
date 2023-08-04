package rsql

import "testing"

func TestString(t *testing.T) {
	program := &Program{
		Statements: []Statement{
			ExpressionStatement{
				Token: Token{TokEQ, "=="},
				Expression: &InfixExpression{
					Token:    Token{TokEQ, "=="},
					Left:     &Identifier{Token{TokIdent, "foo"}, "foo"},
					Operator: "==",
					Right:    &Integer{Token{TokInteger, "5"}, 5},
				},
			},
		},
	}

	t.Log(program.String())
}
