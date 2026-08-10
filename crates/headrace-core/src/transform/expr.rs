//! A closed numeric expression over a record's `value` and numeric attributes, for the
//! `map` transform. Supports the operators `+ - * / %` and `^` (power), unary `-`, and
//! parentheses; operands are number literals, the keyword `value`, and bare attribute
//! names. Deliberately closed - no I/O, branching, or functions - so it parses and
//! validates statically and evaluates cheaply. Arbitrary logic is `wasm`'s job.

use crate::record::{Fault, Record};

/// Caps that keep a hostile or accidentally-huge expression from exhausting the parser:
/// a flat length bound (`MAX_TOKENS`) and a nesting bound (`MAX_DEPTH`, which caps the
/// recursive-descent depth so deep nesting errors instead of overflowing the stack). Real
/// expressions are a handful of tokens and shallow; both limits sit far above any
/// legitimate use and exist only to make parsing a bounded, panic-free operation on input
/// that may come from a less-trusted author once IR authoring is exposed.
const MAX_TOKENS: usize = 1024;
const MAX_DEPTH: usize = 64;

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
        if tokens.len() > MAX_TOKENS {
            return Err(ParseError(format!(
                "expression too long: {} tokens (max {MAX_TOKENS})",
                tokens.len()
            )));
        }
        let mut parser = Parser { tokens, pos: 0 };
        let node = parser.expr(0, 0)?;
        if parser.pos != parser.tokens.len() {
            return Err(ParseError("unexpected trailing input".into()));
        }
        Ok(Expr(node))
    }

    /// Evaluate against `rec`. `Err` distinguishes a referenced field that is absent
    /// ([`Fault::Missing`]) from one that is present but non-numeric ([`Fault::Invalid`]);
    /// arithmetic on present operands yields `Ok`, even when the result is non-finite.
    pub fn eval(&self, rec: &Record) -> Result<f64, Fault> {
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
    fn eval(&self, rec: &Record) -> Result<f64, Fault> {
        match self {
            Node::Num(n) => Ok(*n),
            Node::Value => Ok(rec.value),
            Node::Field(name) => rec.numeric(Some(name)),
            Node::Neg(e) => Ok(-e.eval(rec)?),
            Node::Bin(op, l, r) => {
                let (a, b) = (l.eval(rec)?, r.eval(rec)?);
                Ok(match op {
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
    fn expr(&mut self, min_bp: u8, depth: usize) -> Result<Node, ParseError> {
        if depth > MAX_DEPTH {
            return Err(ParseError(format!(
                "expression nested too deeply (max {MAX_DEPTH})"
            )));
        }
        let mut lhs = self.prefix(depth)?;
        while let Some((op, l_bp, r_bp)) = self.peek().and_then(infix) {
            if l_bp < min_bp {
                break;
            }
            self.pos += 1;
            let rhs = self.expr(r_bp, depth + 1)?;
            lhs = Node::Bin(op, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn prefix(&mut self, depth: usize) -> Result<Node, ParseError> {
        match self.next() {
            Some(Token::Minus) => Ok(Node::Neg(Box::new(self.expr(5, depth + 1)?))),
            Some(Token::Num(n)) => Ok(Node::Num(n)),
            Some(Token::Ident(name)) => Ok(if name == "value" {
                Node::Value
            } else {
                Node::Field(name)
            }),
            Some(Token::LParen) => {
                let inner = self.expr(0, depth + 1)?;
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
    use crate::record::{AttrValue, Attrs};
    use proptest::prelude::*;

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
        assert_eq!(eval("value / 1000", &r), Ok(0.1));
        assert_eq!(eval("errors / total", &r), Ok(0.25));
        assert_eq!(eval("value - errors", &r), Ok(97.0));
    }

    #[test]
    fn distinguishes_missing_from_non_numeric() {
        let r = rec(1.0, &[("errors", 3.0)]);
        assert_eq!(
            eval("errors / total", &r),
            Err(Fault::Missing),
            "total is absent"
        );
        // A present but non-numeric field is Invalid, not Missing.
        let mut attrs = Attrs::new();
        attrs.insert("label".into(), AttrValue::Str("x".into()));
        let r2 = Record {
            ts_nanos: 1,
            start_ts_nanos: None,
            resource: Attrs::new(),
            scope: None,
            name: "m".into(),
            value: 1.0,
            attrs,
        };
        assert_eq!(eval("label + 1", &r2), Err(Fault::Invalid));
    }

    #[test]
    fn rejects_malformed_expressions() {
        for bad in ["", "1 +", "1 + + 2", "(1 + 2", "value $ 2", "1 2"] {
            assert!(Expr::parse(bad).is_err(), "`{bad}` should not parse");
        }
    }

    #[test]
    fn rejects_too_deeply_nested() {
        // Deep nesting must be rejected, not overflow the recursive-descent parser.
        let n = MAX_DEPTH + 5;
        let deep = format!("{}1{}", "(".repeat(n), ")".repeat(n));
        let err = Expr::parse(&deep).expect_err("over-deep expression must be rejected");
        assert!(err.0.contains("nested too deeply"), "got: {}", err.0);
    }

    #[test]
    fn rejects_too_many_tokens() {
        // A very long flat expression is bounded by the token cap before parsing.
        let long = format!("1{}", " + 1".repeat(MAX_TOKENS));
        let err = Expr::parse(&long).expect_err("over-long expression must be rejected");
        assert!(err.0.contains("too long"), "got: {}", err.0);
    }

    #[test]
    fn accepts_reasonable_nesting() {
        // Well under the depth cap still parses and evaluates.
        let ok = format!("{}1 + 2{}", "(".repeat(10), ")".repeat(10));
        assert_eq!(constant(&ok), 3.0);
    }

    proptest! {
        /// Parsing must never panic - any input yields `Ok` or a `ParseError`.
        #[test]
        fn parsing_never_panics(s in ".*") {
            let _ = Expr::parse(&s);
        }

        /// Precedence and grouping match manual evaluation. Integer-valued operands keep
        /// f64 arithmetic exact; the divisor is nonzero.
        #[test]
        fn precedence_matches_manual_evaluation(
            a in 0i64..1000, b in 0i64..1000, c in 1i64..1000,
        ) {
            let (fa, fb, fc) = (a as f64, b as f64, c as f64);
            prop_assert_eq!(constant(&format!("{a} + {b} * {c}")), fa + fb * fc);
            prop_assert_eq!(constant(&format!("({a} + {b}) * {c}")), (fa + fb) * fc);
            prop_assert_eq!(constant(&format!("{a} - {b} - {c}")), fa - fb - fc);
            prop_assert_eq!(constant(&format!("{a} / {c}")), fa / fc);
        }
    }

    // --- helpers ---

    fn eval(expr: &str, rec: &Record) -> Result<f64, Fault> {
        Expr::parse(expr).expect("parses").eval(rec)
    }

    fn constant(expr: &str) -> f64 {
        eval(expr, &rec(0.0, &[])).expect("no fields referenced")
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
