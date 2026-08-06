//! A closed numeric expression over a record's `value` and numeric attributes, for the
//! `map` transform. Supports the operators `+ - * / %` and `^` (power), unary `-`, and
//! parentheses; operands are number literals, the keyword `value`, and bare attribute
//! names. Deliberately closed - no I/O, branching, or functions - so it parses and
//! validates statically and evaluates cheaply. Arbitrary logic is `wasm`'s job.

use crate::record::{AttrValue, Record};

/// A parse failure, with a human-readable reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError(pub String);

/// A parsed, ready-to-evaluate expression.
#[derive(Debug, Clone)]
pub struct Expr(Node);

impl Expr {
    /// Parse `input` into an expression, or report why it is malformed.
    pub fn parse(input: &str) -> Result<Self, ParseError> {
        let tokens = lex(input)?;
        if tokens.is_empty() {
            return Err(ParseError("empty expression".into()));
        }
        let mut parser = Parser { tokens, pos: 0 };
        let node = parser.expr(0)?;
        if parser.pos != parser.tokens.len() {
            return Err(ParseError("unexpected trailing input".into()));
        }
        Ok(Expr(node))
    }

    /// Evaluate against `rec`. `None` if a referenced field is missing or non-numeric;
    /// arithmetic on present operands (including a non-finite result) yields `Some`.
    pub fn eval(&self, rec: &Record) -> Option<f64> {
        self.0.eval(rec)
    }
}

#[derive(Debug, Clone)]
enum Node {
    Num(f64),
    /// The record's `value`.
    Value,
    /// A numeric attribute, by name.
    Field(String),
    Neg(Box<Node>),
    Bin(Op, Box<Node>, Box<Node>),
}

#[derive(Debug, Clone, Copy)]
enum Op {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
    Pow,
}

impl Node {
    fn eval(&self, rec: &Record) -> Option<f64> {
        match self {
            Node::Num(n) => Some(*n),
            Node::Value => Some(rec.value),
            Node::Field(name) => rec.lookup(name).and_then(AttrValue::as_f64),
            Node::Neg(e) => Some(-e.eval(rec)?),
            Node::Bin(op, l, r) => {
                let (a, b) = (l.eval(rec)?, r.eval(rec)?);
                Some(match op {
                    Op::Add => a + b,
                    Op::Sub => a - b,
                    Op::Mul => a * b,
                    Op::Div => a / b,
                    Op::Rem => a % b,
                    Op::Pow => a.powf(b),
                })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Num(f64),
    Ident(String),
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Caret,
    LParen,
    RParen,
}

fn lex(input: &str) -> Result<Vec<Token>, ParseError> {
    let chars: Vec<char> = input.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_ascii_whitespace() => i += 1,
            '+' => push(&mut tokens, Token::Plus, &mut i),
            '-' => push(&mut tokens, Token::Minus, &mut i),
            '*' => push(&mut tokens, Token::Star, &mut i),
            '/' => push(&mut tokens, Token::Slash, &mut i),
            '%' => push(&mut tokens, Token::Percent, &mut i),
            '^' => push(&mut tokens, Token::Caret, &mut i),
            '(' => push(&mut tokens, Token::LParen, &mut i),
            ')' => push(&mut tokens, Token::RParen, &mut i),
            c if c.is_ascii_digit() => {
                let start = i;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
                if i < chars.len() && chars[i] == '.' {
                    i += 1;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                }
                let text: String = chars[start..i].iter().collect();
                let n = text
                    .parse::<f64>()
                    .map_err(|_| ParseError(format!("invalid number `{text}`")))?;
                tokens.push(Token::Num(n));
            }
            // Identifiers allow dots so OTel-style attribute names parse as one token.
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len()
                    && (chars[i].is_ascii_alphanumeric() || chars[i] == '_' || chars[i] == '.')
                {
                    i += 1;
                }
                tokens.push(Token::Ident(chars[start..i].iter().collect()));
            }
            _ => return Err(ParseError(format!("unexpected character `{c}`"))),
        }
    }
    Ok(tokens)
}

fn push(tokens: &mut Vec<Token>, token: Token, i: &mut usize) {
    tokens.push(token);
    *i += 1;
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.pos).cloned();
        self.pos += 1;
        token
    }

    /// Pratt parse with binding powers. `+ -` bind loosest, then `* / %`, then `^`
    /// (right-associative); unary `-` binds between `* / %` and `^`.
    fn expr(&mut self, min_bp: u8) -> Result<Node, ParseError> {
        let mut lhs = self.prefix()?;
        while let Some((op, l_bp, r_bp)) = self.peek().and_then(infix) {
            if l_bp < min_bp {
                break;
            }
            self.pos += 1;
            let rhs = self.expr(r_bp)?;
            lhs = Node::Bin(op, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn prefix(&mut self) -> Result<Node, ParseError> {
        match self.next() {
            Some(Token::Minus) => Ok(Node::Neg(Box::new(self.expr(5)?))),
            Some(Token::Num(n)) => Ok(Node::Num(n)),
            Some(Token::Ident(name)) => Ok(if name == "value" {
                Node::Value
            } else {
                Node::Field(name)
            }),
            Some(Token::LParen) => {
                let inner = self.expr(0)?;
                match self.next() {
                    Some(Token::RParen) => Ok(inner),
                    _ => Err(ParseError("expected `)`".into())),
                }
            }
            other => Err(ParseError(format!("unexpected token: {other:?}"))),
        }
    }
}

/// `(op, left_bp, right_bp)` for a binary operator. Left-associative ops have
/// `left_bp < right_bp`; `^` is right-associative (`left_bp > right_bp`).
fn infix(token: &Token) -> Option<(Op, u8, u8)> {
    Some(match token {
        Token::Plus => (Op::Add, 1, 2),
        Token::Minus => (Op::Sub, 1, 2),
        Token::Star => (Op::Mul, 3, 4),
        Token::Slash => (Op::Div, 3, 4),
        Token::Percent => (Op::Rem, 3, 4),
        Token::Caret => (Op::Pow, 7, 6),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Attrs;

    fn eval(expr: &str, rec: &Record) -> Option<f64> {
        Expr::parse(expr).expect("parses").eval(rec)
    }

    fn constant(expr: &str) -> f64 {
        eval(expr, &rec(0.0, &[])).expect("no fields referenced")
    }

    #[test]
    fn arithmetic_precedence_and_associativity() {
        assert_eq!(constant("1 + 2 * 3"), 7.0);
        assert_eq!(constant("(1 + 2) * 3"), 9.0);
        assert_eq!(constant("10 - 2 - 3"), 5.0); // left-associative
        assert_eq!(constant("2 ^ 3 ^ 2"), 512.0); // right-associative: 2^(3^2)
        assert_eq!(constant("10 % 3"), 1.0);
        assert_eq!(constant("-2 ^ 2"), -4.0); // power binds tighter than unary minus
        assert_eq!(constant("-2 * 3"), -6.0);
    }

    #[test]
    fn reads_value_and_fields() {
        let r = rec(100.0, &[("errors", 3.0), ("total", 12.0)]);
        assert_eq!(eval("value / 1000", &r), Some(0.1));
        assert_eq!(eval("errors / total", &r), Some(0.25));
        assert_eq!(eval("value - errors", &r), Some(97.0));
    }

    #[test]
    fn missing_or_non_numeric_field_is_none() {
        let r = rec(1.0, &[("errors", 3.0)]);
        assert_eq!(eval("errors / total", &r), None, "total is absent");
    }

    #[test]
    fn rejects_malformed_expressions() {
        for bad in ["", "1 +", "1 + + 2", "(1 + 2", "value $ 2", "1 2"] {
            assert!(Expr::parse(bad).is_err(), "`{bad}` should not parse");
        }
    }

    fn rec(value: f64, fields: &[(&str, f64)]) -> Record {
        let mut attrs = Attrs::new();
        for (k, v) in fields {
            attrs.insert((*k).to_string(), AttrValue::Double(*v));
        }
        Record {
            ts_nanos: 1,
            start_ts_nanos: None,
            resource: Attrs::new(),
            scope: None,
            name: "m".into(),
            value,
            attrs,
        }
    }
}
