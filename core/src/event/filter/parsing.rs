//! This module contains the parser for the filter expression language.
//! It uses the `winnow` library for parsing and defines the grammar for the expression language.
//! The parser converts the input string into an abstract syntax tree (AST) representation of the expression.

use super::ast::{
    Accessor, ArithmeticExpr, ArithmeticOperator, ComparisonOperator, Condition, ConditionLeft,
    Expression, LiteralValue, LogicalOperator, VariablePath, VariableSource,
};
use winnow::{
    ascii::{digit1, space0, space1},
    combinator::{alt, delimited, eof, opt, peek, repeat, Repeat},
    error::{ContextError, ErrMode, ParseError, StrContext, StrContextValue},
    prelude::*,
    token::{literal, one_of, take_while},
};

/// --- Helper aliases ---
type Input<'a> = &'a str;
/// Result for internal parser functions
type ParserResult<T> = winnow::Result<T, ErrMode<ContextError>>;

// Helper to check for keywords
// These words cannot be used as unquoted string literals or variable names
fn is_keyword(ident: &str) -> bool {
    matches!(ident.to_ascii_lowercase().as_str(), "true" | "false")
}

/// Common delimiters that can follow a literal value
const COMMON_DELIMITERS: [char; 14] =
    [')', '(', ',', '=', '!', '>', '<', '&', '|', ']', '+', '-', '*', '/'];

/// --- Parser functions ---
/// Parses boolean literals into `LiteralValue::Bool`
fn parse_boolean<'a>(input: &mut Input<'a>) -> ParserResult<LiteralValue<'a>> {
    let parse_true = (
        literal("true"),
        peek(alt((
            // Ensure "true" is followed by a delimiter or EOF
            space1.value(()),
            eof.value(()),
            one_of(COMMON_DELIMITERS).value(()),
        ))),
    )
        .map(|_| LiteralValue::Bool(true));

    let parse_false = (
        literal("false"),
        peek(alt((
            // Ensure "false" is followed by a delimiter or EOF
            space1.value(()),
            eof.value(()),
            one_of(COMMON_DELIMITERS).value(()),
        ))),
    )
        .map(|_| LiteralValue::Bool(false));

    alt((parse_true, parse_false))
        .context(StrContext::Expected(StrContextValue::Description(
            "boolean literal 'true' or 'false'",
        )))
        .parse_next(input)
}

/// Parses any numeric-looking literal (integer or float) into LiteralValue::Number(&'a str).
fn parse_number_or_fixed_str<'a>(input: &mut Input<'a>) -> ParserResult<LiteralValue<'a>> {
    (
        opt(one_of(['+', '-'])),
        digit1,
        opt((literal("."), digit1)), // Optional fractional part
        peek(alt((
            // Ensure it's properly delimited
            space1.value(()),
            eof.value(()),
            one_of(COMMON_DELIMITERS).value(()),
        ))),
    )
        .take()
        .map(|s: &str| LiteralValue::Number(s)) // Store as Number(&str)
        .context(StrContext::Expected(StrContextValue::Description(
            "numeric literal (integer or fixed-point)",
        )))
        .parse_next(input)
}

// Parses an unquoted "0x..." or "0X..." sequence as a string.
fn parse_hex_string<'a>(input: &mut Input<'a>) -> ParserResult<LiteralValue<'a>> {
    (
        alt((literal("0x"), literal("0X"))),
        take_while(1.., |c: char| c.is_ascii_hexdigit()), // Ensure at least one hex digit
        peek(alt((space1.value(()), eof.value(()), one_of(COMMON_DELIMITERS).value(())))),
    )
        .take()
        .map(|s: &str| LiteralValue::Str(s))
        .context(StrContext::Expected(StrContextValue::Description("hexadecimal string literal")))
        .parse_next(input)
}

/// Parses string literals enclosed in single or double quotes into `LiteralValue::Str`
fn parse_quoted_string<'a>(input: &mut Input<'a>) -> ParserResult<LiteralValue<'a>> {
    // Match and consume opening quote, remember which one it was.
    let open_quote: char = one_of(['\'', '"']).parse_next(input)?;

    let character_or_escape_sequence = alt((
        (literal("\\"), one_of([open_quote, '\\'])).void(),
        take_while(1.., move |c: char| c != open_quote && c != '\\').void(),
    ));

    let inner_parser: Repeat<_, &_, _, (), _> = repeat(0.., character_or_escape_sequence);

    let string_content_slice: &'a str = inner_parser.take().parse_next(input)?;

    literal(open_quote)
        .context(StrContext::Expected(StrContextValue::Description(
            "matching closing quote for string literal",
        )))
        .parse_next(input)?;

    Ok(LiteralValue::Str(string_content_slice))
}

/// Parses an accessor (either an index or a key) from the input
fn parse_accessor<'a>(input: &mut Input<'a>) -> ParserResult<Accessor<'a>> {
    let index_parser = delimited(
        literal("["),
        // digit1 itself returns &str, try_map converts it
        digit1.try_map(|s: &str| s.parse::<usize>()),
        literal("]"),
    )
    .map(Accessor::Index)
    .context(StrContext::Expected(StrContextValue::Description("array index accessor like '[0]'")));

    let key_parser = (
        literal("."),
        // Allow key to be purely numeric OR start with alpha/_
        alt((
            // Standard identifier-like key
            (
                one_of(|c: char| c.is_alpha() || c == '_'),
                take_while(0.., |c: char| c.is_alphanum() || c == '_'),
            )
                .take(),
            // Purely numeric key (e.g., ".0", ".123")
            digit1.take(),
        )),
        // Ensure it's properly delimited
        peek(alt((
            space1.value(()),                                 // space
            eof.value(()),                                    // end of input
            literal("[").value(()),                           // start of index accessor
            literal(".").value(()),                           // start of another key accessor
            one_of(['=', '!', '>', '<', ')', '(']).value(()), // Operators or delimiters
        ))),
    )
        .map(|(_, key_slice, _): (_, &str, _)| Accessor::Key(key_slice))
        .context(StrContext::Expected(StrContextValue::Description(
            "object key accessor like '.key' or '.0'",
        )));

    alt((index_parser, key_parser)).parse_next(input)
}

/// Parses a variable name with optional prefix ($ for event, @ for table).
/// Returns the variable name (without prefix) and its source.
fn parse_base_variable_name<'a>(input: &mut Input<'a>) -> ParserResult<(&'a str, VariableSource)> {
    (
        // Check for @ (table) or $ (event) prefix
        opt(alt((literal("@"), literal("$")))),
        alt((
            // Standard identifier
            (
                one_of(|c: char| c.is_alpha() || c == '_'),
                take_while(0.., |c: char| c.is_alphanum() || c == '_'),
            )
                .take(),
            // Purely numeric identifier
            (
                digit1,
                peek(alt((
                    // Peek ensures it's properly delimited for an LHS base
                    literal('['),
                    literal('.'),
                    space1,
                    eof,
                    literal("=="),
                    literal("!="),
                    literal(">="),
                    literal("<="),
                    literal(">"),
                    literal("<"),
                    literal("+"),
                    literal("-"),
                    literal("*"),
                    literal("/"),
                    literal(")"),
                ))),
            )
                .take(),
        )),
    )
        .verify(|(_, ident_slice): &(_, &str)| !is_keyword(ident_slice))
        .map(|(prefix, name): (Option<&str>, &str)| {
            let source = match prefix {
                Some("@") => VariableSource::Table,
                _ => VariableSource::Event, // $ or no prefix = event
            };
            (name, source)
        })
        .context(StrContext::Expected(StrContextValue::Description(
            "variable base name (e.g., 'request', '$value', or '@balance')",
        )))
        .parse_next(input)
}

fn parse_condition_lhs<'a>(input: &mut Input<'a>) -> ParserResult<ConditionLeft<'a>> {
    // Parse the base variable name and its source
    let (base, source) = parse_base_variable_name.parse_next(input)?;

    // Parse any accessors (e.g., .key or [0])
    let accessors: Vec<Accessor> = repeat(0.., parse_accessor).parse_next(input)?;

    if accessors.is_empty() {
        Ok(ConditionLeft::Simple(base, source))
    } else {
        Ok(ConditionLeft::Path(VariablePath { base, accessors, source }))
    }
}

/// Parses a literal value without unquoted strings (for use in arithmetic expressions).
/// This prevents variable names from being matched as literals.
fn parse_numeric_or_quoted_literal<'a>(input: &mut Input<'a>) -> ParserResult<LiteralValue<'a>> {
    delimited(
        space0,
        alt((
            parse_quoted_string, // "'string'" or '"string"'
            parse_boolean,       // "true" / "false"
            parse_hex_string,    // "0x..."
            parse_number_or_fixed_str, // "123" / "-123" / "123.456"
                                 // Note: parse_unquoted_string is intentionally excluded here
        )),
        space0,
    )
    .context(StrContext::Expected(StrContextValue::Description(
        "boolean, number, hex string or quoted string",
    )))
    .parse_next(input)
}

/// Parses a comparison operator (e.g., ==, !=, >, >=, <, <=)
/// Handles optional whitespace around the operator
fn parse_comparison_operator(input: &mut Input<'_>) -> ParserResult<ComparisonOperator> {
    delimited(
        space0,
        alt((
            literal(">=").map(|_| ComparisonOperator::Gte),
            literal("<=").map(|_| ComparisonOperator::Lte),
            literal("==").map(|_| ComparisonOperator::Eq),
            literal("!=").map(|_| ComparisonOperator::Ne),
            literal(">").map(|_| ComparisonOperator::Gt),
            literal("<").map(|_| ComparisonOperator::Lt),
        )),
        space0,
    )
    .context(StrContext::Expected(StrContextValue::Description(
        "comparison operator (e.g., ==, >, >=, <, <=, !=)",
    )))
    .parse_next(input)
}

/// Parses an additive operator (+ or -)
fn parse_additive_operator(input: &mut Input<'_>) -> ParserResult<ArithmeticOperator> {
    delimited(
        space0,
        alt((
            literal("+").map(|_| ArithmeticOperator::Add),
            literal("-").map(|_| ArithmeticOperator::Subtract),
        )),
        space0,
    )
    .context(StrContext::Expected(StrContextValue::Description("additive operator (+ or -)")))
    .parse_next(input)
}

/// Parses a multiplicative operator (* or /)
fn parse_multiplicative_operator(input: &mut Input<'_>) -> ParserResult<ArithmeticOperator> {
    delimited(
        space0,
        alt((
            literal("*").map(|_| ArithmeticOperator::Multiply),
            literal("/").map(|_| ArithmeticOperator::Divide),
        )),
        space0,
    )
    .context(StrContext::Expected(StrContextValue::Description("multiplicative operator (* or /)")))
    .parse_next(input)
}

/// Parses a primary arithmetic operand: variable, literal, or parenthesized arithmetic expression
fn parse_arithmetic_primary<'a>(input: &mut Input<'a>) -> ParserResult<ArithmeticExpr<'a>> {
    delimited(
        space0,
        alt((
            // Parenthesized arithmetic expression
            delimited((literal("("), space0), parse_arithmetic_expr, (space0, literal(")"))),
            // Numeric/quoted literals - try this first to match numbers like "25" before variable names
            // Uses parse_numeric_or_quoted_literal to avoid matching identifiers as unquoted strings
            parse_numeric_or_quoted_literal.map(ArithmeticExpr::Literal),
            // Variable reference - try this after numeric literals
            parse_condition_lhs.map(ArithmeticExpr::Variable),
        )),
        space0,
    )
    .context(StrContext::Expected(StrContextValue::Description(
        "arithmetic operand (variable, number, or parenthesized expression)",
    )))
    .parse_next(input)
}

/// Parses multiplicative expressions (* and /) - higher precedence
fn parse_multiplicative_expr<'a>(input: &mut Input<'a>) -> ParserResult<ArithmeticExpr<'a>> {
    let left = parse_arithmetic_primary.parse_next(input)?;

    let trailing_parser = (parse_multiplicative_operator, parse_arithmetic_primary);

    let folded_parser = repeat(0.., trailing_parser).fold(
        move || left.clone(),
        |acc, (op, right)| ArithmeticExpr::Binary {
            left: Box::new(acc),
            operator: op,
            right: Box::new(right),
        },
    );

    folded_parser
        .context(StrContext::Expected(StrContextValue::Description("multiplicative expression")))
        .parse_next(input)
}

/// Parses additive expressions (+ and -) - lower precedence than multiplicative
fn parse_additive_expr<'a>(input: &mut Input<'a>) -> ParserResult<ArithmeticExpr<'a>> {
    let left = parse_multiplicative_expr.parse_next(input)?;

    let trailing_parser = (parse_additive_operator, parse_multiplicative_expr);

    let folded_parser = repeat(0.., trailing_parser).fold(
        move || left.clone(),
        |acc, (op, right)| ArithmeticExpr::Binary {
            left: Box::new(acc),
            operator: op,
            right: Box::new(right),
        },
    );

    folded_parser
        .context(StrContext::Expected(StrContextValue::Description("additive expression")))
        .parse_next(input)
}

/// Parses a complete arithmetic expression
fn parse_arithmetic_expr<'a>(input: &mut Input<'a>) -> ParserResult<ArithmeticExpr<'a>> {
    parse_additive_expr.parse_next(input)
}

/// Parses a condition expression (e.g., "a == 1" or "value + fee > balance * 2")
fn parse_condition<'a>(input: &mut Input<'a>) -> ParserResult<Expression<'a>> {
    let (left, operator, right) =
        (parse_arithmetic_expr, parse_comparison_operator, parse_arithmetic_expr)
            .context(StrContext::Expected(StrContextValue::Description(
                "condition expression (e.g., variable == value or value + 1 > 100)",
            )))
            .parse_next(input)?;

    let condition = Condition { left, operator, right };

    Ok(Expression::Condition(condition))
}

/// Parses the highest precedence components: conditions and parenthesized expressions
fn parse_term<'a>(input: &mut Input<'a>) -> ParserResult<Expression<'a>> {
    delimited(
        space0,
        alt((
            // Parse a parenthesized expression
            delimited(
                (literal("("), space0),
                parse_expression,
                (space0, literal(")")).context(StrContext::Expected(StrContextValue::Description(
                    "closing parenthesis ')'",
                ))),
            ),
            // Parse a condition
            parse_condition,
        )),
        space0,
    )
    .context(StrContext::Expected(StrContextValue::Description(
        "condition or parenthesized expression",
    )))
    .parse_next(input)
}

/// Parses the AND operator and its components
fn parse_and_expression<'a>(input: &mut Input<'a>) -> ParserResult<Expression<'a>> {
    let left = parse_term.parse_next(input)?;

    let and_operator_parser = delimited(space0, literal("&&").value(LogicalOperator::And), space0)
        .context(StrContext::Expected(StrContextValue::Description("logical operator &&")));

    let trailing_parser = (and_operator_parser, parse_term);

    let folded_and_parser = repeat(0.., trailing_parser).fold(
        move || left.clone(), // Clone the left side for initial value
        |acc, (op, right)| Expression::Logical {
            left: Box::new(acc),
            operator: op,
            right: Box::new(right),
        },
    );

    folded_and_parser
        .context(StrContext::Expected(StrContextValue::Description("AND expression")))
        .parse_next(input)
}

/// Parses the OR operator and its components
fn parse_or_expression<'a>(input: &mut Input<'a>) -> ParserResult<Expression<'a>> {
    let left = parse_and_expression.parse_next(input)?;
    let or_operator_parser = delimited(space0, literal("||").value(LogicalOperator::Or), space0)
        .context(StrContext::Expected(StrContextValue::Description("logical operator ||")));
    let trailing_parser = (or_operator_parser, parse_and_expression);
    let folded_or_parser = repeat(0.., trailing_parser).fold(
        move || left.clone(),
        |acc, (op, right)| Expression::Logical {
            left: Box::new(acc),
            operator: op,
            right: Box::new(right),
        },
    );
    folded_or_parser
        .context(StrContext::Expected(StrContextValue::Description("OR expression")))
        .parse_next(input)
}

/// Parses the entire expression, starting from the highest precedence
fn parse_expression<'a>(input: &mut Input<'a>) -> ParserResult<Expression<'a>> {
    delimited(space0, parse_or_expression, space0)
        .context(StrContext::Expected(StrContextValue::Description("a full expression")))
        .parse_next(input)
}

/// Public method, which parses a string expression into an `Expression` AST
pub fn parse(expression_str: &str) -> Result<Expression<'_>, ParseError<Input<'_>, ContextError>> {
    // Parse the expression and ensure it ends with EOF
    let mut full_expression_parser = (parse_expression, eof).map(|(expr, _)| expr);

    full_expression_parser.parse(expression_str)
}

/// Parses a base variable name with optional $ prefix (for computed columns).
/// Parses a variable name with optional prefix ($ for event, @ for table) for arithmetic expressions.
/// Returns the variable name (without prefix) and its source.
fn parse_base_variable_name_with_dollar<'a>(
    input: &mut Input<'a>,
) -> ParserResult<(&'a str, VariableSource)> {
    (
        // Check for @ (table) or $ (event) prefix
        opt(alt((literal("@"), literal("$")))),
        alt((
            // Standard identifier
            (
                one_of(|c: char| c.is_alpha() || c == '_'),
                take_while(0.., |c: char| c.is_alphanum() || c == '_'),
            )
                .take(),
            // Purely numeric identifier
            (
                digit1,
                peek(alt((
                    literal('['),
                    literal('.'),
                    space1,
                    eof,
                    literal("+"),
                    literal("-"),
                    literal("*"),
                    literal("/"),
                    literal(")"),
                ))),
            )
                .take(),
        )),
    )
        .verify(|(_, ident_slice): &(_, &str)| !is_keyword(ident_slice))
        .map(|(prefix, name): (Option<&str>, &str)| {
            let source = match prefix {
                Some("@") => VariableSource::Table,
                _ => VariableSource::Event, // $ or no prefix = event
            };
            (name, source)
        })
        .context(StrContext::Expected(StrContextValue::Description(
            "variable name (with $, @, or no prefix)",
        )))
        .parse_next(input)
}

/// Parses a condition LHS that allows optional $ or @ prefix on variable names.
fn parse_condition_lhs_with_dollar<'a>(input: &mut Input<'a>) -> ParserResult<ConditionLeft<'a>> {
    // Parse the base variable name and its source
    let (base, source) = parse_base_variable_name_with_dollar.parse_next(input)?;

    // Parse any accessors (e.g., .key or [0])
    let accessors: Vec<Accessor> = repeat(0.., parse_accessor).parse_next(input)?;

    if accessors.is_empty() {
        Ok(ConditionLeft::Simple(base, source))
    } else {
        Ok(ConditionLeft::Path(VariablePath { base, accessors, source }))
    }
}

/// Parses a primary arithmetic operand with $ prefix support: variable, literal, or parenthesized expression
fn parse_arithmetic_primary_with_dollar<'a>(
    input: &mut Input<'a>,
) -> ParserResult<ArithmeticExpr<'a>> {
    delimited(
        space0,
        alt((
            // Parenthesized arithmetic expression
            delimited(
                (literal("("), space0),
                parse_arithmetic_expr_with_dollar,
                (space0, literal(")")),
            ),
            // Numeric/quoted literals - try this first to match numbers before variables
            parse_numeric_or_quoted_literal.map(ArithmeticExpr::Literal),
            // Variable reference (with optional $) - try after numeric literals
            parse_condition_lhs_with_dollar.map(ArithmeticExpr::Variable),
        )),
        space0,
    )
    .context(StrContext::Expected(StrContextValue::Description(
        "arithmetic operand (variable with optional $, number, or parenthesized expression)",
    )))
    .parse_next(input)
}

/// Parses multiplicative expressions with $ prefix support
fn parse_multiplicative_expr_with_dollar<'a>(
    input: &mut Input<'a>,
) -> ParserResult<ArithmeticExpr<'a>> {
    let left = parse_arithmetic_primary_with_dollar.parse_next(input)?;

    let trailing_parser = (parse_multiplicative_operator, parse_arithmetic_primary_with_dollar);

    let folded_parser = repeat(0.., trailing_parser).fold(
        move || left.clone(),
        |acc, (op, right)| ArithmeticExpr::Binary {
            left: Box::new(acc),
            operator: op,
            right: Box::new(right),
        },
    );

    folded_parser
        .context(StrContext::Expected(StrContextValue::Description("multiplicative expression")))
        .parse_next(input)
}

/// Parses additive expressions with $ prefix support
fn parse_additive_expr_with_dollar<'a>(input: &mut Input<'a>) -> ParserResult<ArithmeticExpr<'a>> {
    let left = parse_multiplicative_expr_with_dollar.parse_next(input)?;

    let trailing_parser = (parse_additive_operator, parse_multiplicative_expr_with_dollar);

    let folded_parser = repeat(0.., trailing_parser).fold(
        move || left.clone(),
        |acc, (op, right)| ArithmeticExpr::Binary {
            left: Box::new(acc),
            operator: op,
            right: Box::new(right),
        },
    );

    folded_parser
        .context(StrContext::Expected(StrContextValue::Description("additive expression")))
        .parse_next(input)
}

/// Parses a complete arithmetic expression with $ prefix support
fn parse_arithmetic_expr_with_dollar<'a>(
    input: &mut Input<'a>,
) -> ParserResult<ArithmeticExpr<'a>> {
    parse_additive_expr_with_dollar.parse_next(input)
}

/// Parses a standalone arithmetic expression (for computed columns).
/// This parses expressions like "$value * 2", "$amount + $fee", or "$ratio / 100".
///
/// The input can contain variable references with $ prefix to reference event fields.
/// For example: "$value", "$amount / $total", "$fee + $tip * 2"
pub fn parse_arithmetic_expression(
    expression_str: &str,
) -> Result<ArithmeticExpr<'_>, ParseError<Input<'_>, ContextError>> {
    let mut full_arith_parser = (parse_arithmetic_expr_with_dollar, eof).map(|(expr, _)| expr);
    full_arith_parser.parse(expression_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    // helpers
    fn assert_parses_ok<'a, O, P>(
        mut parser: P,
        input: &'a str,
        expected_output: O,
        expected_remaining: &str,
    ) where
        P: FnMut(&mut Input<'a>) -> ParserResult<O>,
        O: PartialEq + std::fmt::Debug,
    {
        let mut mutable_input = input;
        match parser.parse_next(&mut mutable_input) {
            Ok(output) => {
                assert_eq!(output, expected_output, "Output mismatch for input: '{input}'");
                assert_eq!(
                    mutable_input, expected_remaining,
                    "Remaining input mismatch for input: '{input}'"
                );
            }
            Err(e) => panic!("Parser failed for input '{input}': {e:?}"),
        }
    }

    fn assert_parse_fails<'a, O, P>(mut parser: P, input: &'a str)
    where
        P: FnMut(&mut Input<'a>) -> ParserResult<O>,
        O: PartialEq + std::fmt::Debug,
    {
        let mut mutable_input = input;
        assert!(
            parser.parse_next(&mut mutable_input).is_err(),
            "Parser should have failed for input: '{input}'"
        );
    }

    #[test]
    fn test_parse_boolean() {
        // Success cases
        assert_parses_ok(parse_boolean, "true", LiteralValue::Bool(true), "");
        assert_parses_ok(parse_boolean, "false", LiteralValue::Bool(false), "");
        assert_parses_ok(parse_boolean, "true ", LiteralValue::Bool(true), " "); // Consumes only "true"

        // Failures
        assert_parse_fails(parse_boolean, "TRUE"); // Case-sensitive
        assert_parse_fails(parse_boolean, "tru");
        assert_parse_fails(parse_boolean, "  true"); // Does not consume leading space
    }

    #[test]
    fn test_parse_number_or_fixed_str() {
        // Success cases
        assert_parses_ok(parse_number_or_fixed_str, "123", LiteralValue::Number("123"), "");
        assert_parses_ok(parse_number_or_fixed_str, "-456", LiteralValue::Number("-456"), "");
        assert_parses_ok(parse_number_or_fixed_str, "0.5", LiteralValue::Number("0.5"), "");
        assert_parses_ok(parse_number_or_fixed_str, "123.456", LiteralValue::Number("123.456"), "");
        assert_parses_ok(parse_number_or_fixed_str, "-0.789", LiteralValue::Number("-0.789"), "");
        assert_parses_ok(parse_number_or_fixed_str, "123 ", LiteralValue::Number("123"), " "); // Peek space
        assert_parses_ok(parse_number_or_fixed_str, "123)", LiteralValue::Number("123"), ")"); // Peek delimiter
        assert_parses_ok(parse_number_or_fixed_str, "123.45)", LiteralValue::Number("123.45"), ")");

        // Failures
        assert_parse_fails(parse_number_or_fixed_str, "abc");
        assert_parse_fails(parse_number_or_fixed_str, "123a"); // Not delimited
        assert_parse_fails(parse_number_or_fixed_str, "1.2.3"); // Invalid number
        assert_parse_fails(parse_number_or_fixed_str, ".5"); // Requires digit before .
        assert_parse_fails(parse_number_or_fixed_str, "5."); // Requires digit after .
    }

    #[test]
    fn test_parse_hex_string() {
        // Success cases
        assert_parses_ok(parse_hex_string, "0x1a2B", LiteralValue::Str("0x1a2B"), "");
        assert_parses_ok(parse_hex_string, "0XFF", LiteralValue::Str("0XFF"), "");
        assert_parses_ok(
            parse_hex_string,
            "0xabcdef0123456789",
            LiteralValue::Str("0xabcdef0123456789"),
            "",
        );
        assert_parses_ok(parse_hex_string, "0x1 ", LiteralValue::Str("0x1"), " ");
        assert_parses_ok(parse_hex_string, "0xa)", LiteralValue::Str("0xa"), ")");

        // Failures
        assert_parse_fails(parse_hex_string, "0x"); // No digits
        assert_parse_fails(parse_hex_string, "0xG"); // Invalid hex digit
        assert_parse_fails(parse_hex_string, "123"); // Not a hex string
        assert_parse_fails(parse_hex_string, "0x123z"); // Not delimited properly
    }

    #[test]
    fn test_parse_quoted_string() {
        // Success cases
        assert_parses_ok(parse_quoted_string, "'hello'", LiteralValue::Str("hello"), "");
        // Empty string
        assert_parses_ok(parse_quoted_string, "''", LiteralValue::Str(""), "");
        assert_parses_ok(
            parse_quoted_string,
            "'hello world'",
            LiteralValue::Str("hello world"),
            "",
        );
        assert_parses_ok(parse_quoted_string, "'foo\\'bar'", LiteralValue::Str("foo\\'bar"), "");
        assert_parses_ok(parse_quoted_string, "'foo\\\\bar'", LiteralValue::Str("foo\\\\bar"), "");
        assert_parses_ok(parse_quoted_string, "'a\\''", LiteralValue::Str("a\\'"), "");
        // Just an escaped quote
        assert_parses_ok(parse_quoted_string, "'\\''", LiteralValue::Str("\\'"), "");
        // Escaped double quotes
        assert_parses_ok(parse_quoted_string, "'\"hello\"'", LiteralValue::Str("\"hello\""), "");
        assert_parses_ok(parse_quoted_string, "'_'", LiteralValue::Str("_"), "");

        // Failures
        assert_parse_fails(parse_quoted_string, "'hello"); // Missing closing quote
        assert_parse_fails(parse_quoted_string, "hello'"); // Missing opening quote
        assert_parse_fails(parse_quoted_string, "'hello\\"); // Ends with backslash (incomplete escape)
    }

    #[test]
    fn test_is_keyword() {
        // Success cases
        assert!(is_keyword("true"));
        assert!(is_keyword("FALSE"));
        // Failures
        assert!(!is_keyword("trueish"));
        assert!(!is_keyword("variable"));
    }

    #[test]
    fn test_parse_accessor() {
        // Success cases
        assert_parses_ok(parse_accessor, "[123]", Accessor::Index(123), "");
        assert_parses_ok(parse_accessor, ".keyName", Accessor::Key("keyName"), "");
        assert_parses_ok(parse_accessor, "._key_Name0", Accessor::Key("_key_Name0"), "");
        assert_parses_ok(parse_accessor, "[0].next", Accessor::Index(0), ".next");
        // Numeric keys
        assert_parses_ok(parse_accessor, ".0", Accessor::Key("0"), "");
        assert_parses_ok(parse_accessor, ".123", Accessor::Key("123"), "");
        assert_parses_ok(parse_accessor, ".0.next", Accessor::Key("0"), ".next"); // Numeric key followed by another accessor
        assert_parses_ok(parse_accessor, ".45[0]", Accessor::Key("45"), "[0]"); // Numeric key followed by index

        // Failures
        assert_parse_fails(parse_accessor, "keyName"); // Missing .
        assert_parse_fails(parse_accessor, "[abc]"); // Index not a number
        assert_parse_fails(parse_accessor, "[]"); // Empty index
        assert_parse_fails(parse_accessor, ".1key"); // Key cannot start with digit
        assert_parse_fails(parse_accessor, ".key-name"); // Hyphen not allowed in key
    }

    #[test]
    fn test_parse_base_variable_name() {
        // Basic event variables (no prefix or $ prefix)
        assert_parses_ok(
            parse_base_variable_name,
            "request",
            ("request", VariableSource::Event),
            "",
        );
        assert_parses_ok(
            parse_base_variable_name,
            "_privateVar",
            ("_privateVar", VariableSource::Event),
            "",
        );
        assert_parses_ok(parse_base_variable_name, "var123", ("var123", VariableSource::Event), "");
        assert_parses_ok(
            parse_base_variable_name,
            "response ",
            ("response", VariableSource::Event),
            " ",
        );
        assert_parses_ok(parse_base_variable_name, "0", ("0", VariableSource::Event), ""); // Numeric LHS base
        assert_parses_ok(parse_base_variable_name, "123[", ("123", VariableSource::Event), "["); // Numeric LHS base, peek '['
        assert_parses_ok(
            parse_base_variable_name,
            "45.field",
            ("45", VariableSource::Event),
            ".field",
        ); // Numeric LHS base, peek '.'
        assert_parses_ok(parse_base_variable_name, "123 ==", ("123", VariableSource::Event), " ==");

        // $ prefix support - the $ is consumed but not included in result, source is Event
        assert_parses_ok(parse_base_variable_name, "$value", ("value", VariableSource::Event), "");
        assert_parses_ok(parse_base_variable_name, "$from", ("from", VariableSource::Event), "");
        assert_parses_ok(
            parse_base_variable_name,
            "$_private",
            ("_private", VariableSource::Event),
            "",
        );
        assert_parses_ok(
            parse_base_variable_name,
            "$var123 ==",
            ("var123", VariableSource::Event),
            " ==",
        );

        // @ prefix support - table references
        assert_parses_ok(
            parse_base_variable_name,
            "@balance",
            ("balance", VariableSource::Table),
            "",
        );
        assert_parses_ok(
            parse_base_variable_name,
            "@last_updated",
            ("last_updated", VariableSource::Table),
            "",
        );
        assert_parses_ok(
            parse_base_variable_name,
            "@count ==",
            ("count", VariableSource::Table),
            " ==",
        );

        assert_parse_fails(parse_base_variable_name, "true"); // Keyword
        assert_parse_fails(parse_base_variable_name, "123true"); // Invalid identifier
        assert_parse_fails(parse_base_variable_name, "123_"); // underscore after numeric not part of it
    }

    #[test]
    fn test_parse_condition_lhs() {
        assert_parses_ok(
            parse_condition_lhs,
            "var",
            ConditionLeft::Simple("var", VariableSource::Event),
            "",
        );
        assert_parses_ok(
            parse_condition_lhs,
            "var.key",
            ConditionLeft::Path(VariablePath {
                base: "var",
                accessors: vec![Accessor::Key("key")],
                source: VariableSource::Event,
            }),
            "",
        );
        assert_parses_ok(
            parse_condition_lhs,
            "arr[0]",
            ConditionLeft::Path(VariablePath {
                base: "arr",
                accessors: vec![Accessor::Index(0)],
                source: VariableSource::Event,
            }),
            "",
        );
        assert_parses_ok(
            parse_condition_lhs,
            "obj.arr[1].field",
            ConditionLeft::Path(VariablePath {
                base: "obj",
                accessors: vec![Accessor::Key("arr"), Accessor::Index(1), Accessor::Key("field")],
                source: VariableSource::Event,
            }),
            "",
        );
        assert_parses_ok(
            parse_condition_lhs,
            "0.field",
            ConditionLeft::Path(VariablePath {
                base: "0",
                accessors: vec![Accessor::Key("field")],
                source: VariableSource::Event,
            }),
            "",
        );
        assert_parses_ok(
            parse_condition_lhs,
            "obj.0",
            ConditionLeft::Path(VariablePath {
                base: "obj",
                accessors: vec![Accessor::Key("0")],
                source: VariableSource::Event,
            }),
            "",
        );
        assert_parses_ok(
            parse_condition_lhs,
            "0.1", // e.g. base_param_named_0.field_named_1
            ConditionLeft::Path(VariablePath {
                base: "0",
                accessors: vec![Accessor::Key("1")],
                source: VariableSource::Event,
            }),
            "",
        );
        assert_parses_ok(
            parse_condition_lhs,
            "data.123.field",
            ConditionLeft::Path(VariablePath {
                base: "data",
                accessors: vec![Accessor::Key("123"), Accessor::Key("field")],
                source: VariableSource::Event,
            }),
            "",
        );
        assert_parses_ok(
            parse_condition_lhs,
            "map.0[1].name",
            ConditionLeft::Path(VariablePath {
                base: "map",
                accessors: vec![Accessor::Key("0"), Accessor::Index(1), Accessor::Key("name")],
                source: VariableSource::Event,
            }),
            "",
        );
    }

    #[test]
    fn test_parse_comparison_operator() {
        assert_parses_ok(parse_comparison_operator, "==", ComparisonOperator::Eq, "");
        assert_parses_ok(parse_comparison_operator, "!=", ComparisonOperator::Ne, "");
        assert_parses_ok(parse_comparison_operator, ">", ComparisonOperator::Gt, "");
        assert_parses_ok(parse_comparison_operator, ">=", ComparisonOperator::Gte, "");
        assert_parses_ok(parse_comparison_operator, "<", ComparisonOperator::Lt, "");
        assert_parses_ok(parse_comparison_operator, "<=", ComparisonOperator::Lte, "");
    }

    #[test]
    fn test_parse_condition() {
        let expr = "var == 123";
        let expected = Expression::Condition(Condition {
            left: ArithmeticExpr::Variable(ConditionLeft::Simple("var", VariableSource::Event)),
            operator: ComparisonOperator::Eq,
            right: ArithmeticExpr::Literal(LiteralValue::Number("123")),
        });
        assert_parses_ok(parse_condition, expr, expected, "");

        let expr_path = "obj.count > 0.5";
        let expected_path = Expression::Condition(Condition {
            left: ArithmeticExpr::Variable(ConditionLeft::Path(VariablePath {
                base: "obj",
                accessors: vec![Accessor::Key("count")],
                source: VariableSource::Event,
            })),
            operator: ComparisonOperator::Gt,
            right: ArithmeticExpr::Literal(LiteralValue::Number("0.5")),
        });
        assert_parses_ok(parse_condition, expr_path, expected_path, "");
    }

    #[test]
    fn test_parse_condition_with_dollar_prefix() {
        // $ prefix is stripped from variable names for consistent lookup
        let expr = "$from != $to";
        let expected = Expression::Condition(Condition {
            left: ArithmeticExpr::Variable(ConditionLeft::Simple("from", VariableSource::Event)),
            operator: ComparisonOperator::Ne,
            right: ArithmeticExpr::Variable(ConditionLeft::Simple("to", VariableSource::Event)),
        });
        assert_parses_ok(parse_condition, expr, expected, "");

        // $ with path access
        let expr_path = "$data.amount > 0";
        let expected_path = Expression::Condition(Condition {
            left: ArithmeticExpr::Variable(ConditionLeft::Path(VariablePath {
                base: "data",
                accessors: vec![Accessor::Key("amount")],
                source: VariableSource::Event,
            })),
            operator: ComparisonOperator::Gt,
            right: ArithmeticExpr::Literal(LiteralValue::Number("0")),
        });
        assert_parses_ok(parse_condition, expr_path, expected_path, "");

        // Mixed: with and without $ prefix
        let expr_mixed = "$value > balance";
        let expected_mixed = Expression::Condition(Condition {
            left: ArithmeticExpr::Variable(ConditionLeft::Simple("value", VariableSource::Event)),
            operator: ComparisonOperator::Gt,
            right: ArithmeticExpr::Variable(ConditionLeft::Simple(
                "balance",
                VariableSource::Event,
            )),
        });
        assert_parses_ok(parse_condition, expr_mixed, expected_mixed, "");
    }

    #[test]
    fn test_parse_term_parentheses() {
        let expr = "(var == 123)";
        let inner_cond = Condition {
            left: ArithmeticExpr::Variable(ConditionLeft::Simple("var", VariableSource::Event)),
            operator: ComparisonOperator::Eq,
            right: ArithmeticExpr::Literal(LiteralValue::Number("123")),
        };
        let expected = Expression::Condition(inner_cond.clone()); // The term itself is the condition
        assert_parses_ok(parse_term, expr, expected, "");

        let expr_nested = "( var1 > 10 && var2 < 'abc' )";
        let expected_nested = Expression::Logical {
            left: Box::new(Expression::Condition(Condition {
                left: ArithmeticExpr::Variable(ConditionLeft::Simple(
                    "var1",
                    VariableSource::Event,
                )),
                operator: ComparisonOperator::Gt,
                right: ArithmeticExpr::Literal(LiteralValue::Number("10")),
            })),
            operator: LogicalOperator::And,
            right: Box::new(Expression::Condition(Condition {
                left: ArithmeticExpr::Variable(ConditionLeft::Simple(
                    "var2",
                    VariableSource::Event,
                )),
                operator: ComparisonOperator::Lt,
                right: ArithmeticExpr::Literal(LiteralValue::Str("abc")),
            })),
        };
        // parse_term calls parse_expression for parentheses, parse_expression calls parse_or_expression...
        assert_parses_ok(parse_term, expr_nested, expected_nested, "");
    }

    #[test]
    fn test_parse_logical_expressions() {
        let expr = "a == 1 && b < 2.0";
        let expected = Expression::Logical {
            left: Box::new(Expression::Condition(Condition {
                left: ArithmeticExpr::Variable(ConditionLeft::Simple("a", VariableSource::Event)),
                operator: ComparisonOperator::Eq,
                right: ArithmeticExpr::Literal(LiteralValue::Number("1")),
            })),
            operator: LogicalOperator::And,
            right: Box::new(Expression::Condition(Condition {
                left: ArithmeticExpr::Variable(ConditionLeft::Simple("b", VariableSource::Event)),
                operator: ComparisonOperator::Lt,
                right: ArithmeticExpr::Literal(LiteralValue::Number("2.0")),
            })),
        };
        // Test parse_and_expression directly or parse_expression for full precedence
        assert_parses_ok(parse_expression, expr, expected.clone(), "");
        // Also test with parse(), which adds eof
        assert_eq!(parse(expr).unwrap(), expected);

        let expr_or = "a == 1 || b < 'text'";
        let expected_or = Expression::Logical {
            left: Box::new(Expression::Condition(Condition {
                left: ArithmeticExpr::Variable(ConditionLeft::Simple("a", VariableSource::Event)),
                operator: ComparisonOperator::Eq,
                right: ArithmeticExpr::Literal(LiteralValue::Number("1")),
            })),
            operator: LogicalOperator::Or,
            right: Box::new(Expression::Condition(Condition {
                left: ArithmeticExpr::Variable(ConditionLeft::Simple("b", VariableSource::Event)),
                operator: ComparisonOperator::Lt,
                right: ArithmeticExpr::Literal(LiteralValue::Str("text")),
            })),
        };
        assert_eq!(parse(expr_or).unwrap(), expected_or);

        // Precedence: AND over OR
        let expr_mixed = "a == 1 || b < 2 && c > 3";
        let expected_mixed = Expression::Logical {
            left: Box::new(Expression::Condition(Condition {
                left: ArithmeticExpr::Variable(ConditionLeft::Simple("a", VariableSource::Event)),
                operator: ComparisonOperator::Eq,
                right: ArithmeticExpr::Literal(LiteralValue::Number("1")),
            })),
            operator: LogicalOperator::Or,
            right: Box::new(Expression::Logical {
                left: Box::new(Expression::Condition(Condition {
                    left: ArithmeticExpr::Variable(ConditionLeft::Simple(
                        "b",
                        VariableSource::Event,
                    )),
                    operator: ComparisonOperator::Lt,
                    right: ArithmeticExpr::Literal(LiteralValue::Number("2")),
                })),
                operator: LogicalOperator::And,
                right: Box::new(Expression::Condition(Condition {
                    left: ArithmeticExpr::Variable(ConditionLeft::Simple(
                        "c",
                        VariableSource::Event,
                    )),
                    operator: ComparisonOperator::Gt,
                    right: ArithmeticExpr::Literal(LiteralValue::Number("3")),
                })),
            }),
        };
        assert_eq!(parse(expr_mixed).unwrap(), expected_mixed);

        // Parentheses overriding precedence
        let expr_parens = "(a == 1 || b < 2) && c > 3";
        let expected_parens = Expression::Logical {
            left: Box::new(Expression::Logical {
                left: Box::new(Expression::Condition(Condition {
                    left: ArithmeticExpr::Variable(ConditionLeft::Simple(
                        "a",
                        VariableSource::Event,
                    )),
                    operator: ComparisonOperator::Eq,
                    right: ArithmeticExpr::Literal(LiteralValue::Number("1")),
                })),
                operator: LogicalOperator::Or,
                right: Box::new(Expression::Condition(Condition {
                    left: ArithmeticExpr::Variable(ConditionLeft::Simple(
                        "b",
                        VariableSource::Event,
                    )),
                    operator: ComparisonOperator::Lt,
                    right: ArithmeticExpr::Literal(LiteralValue::Number("2")),
                })),
            }),
            operator: LogicalOperator::And,
            right: Box::new(Expression::Condition(Condition {
                left: ArithmeticExpr::Variable(ConditionLeft::Simple("c", VariableSource::Event)),
                operator: ComparisonOperator::Gt,
                right: ArithmeticExpr::Literal(LiteralValue::Number("3")),
            })),
        };
        assert_eq!(parse(expr_parens).unwrap(), expected_parens);
    }

    #[test]
    fn test_full_parse_with_eof() {
        assert!(parse("var == 123").is_ok());
        assert!(parse("var == 123 && extra_stuff_not_parsed").is_err()); // Fails eof
        assert!(parse("(a == 1 || b < 2)&& c > 3").is_ok()); // No space around AND
    }

    #[test]
    fn test_parse_table_reference_with_at_prefix() {
        // @balance should parse as a table reference
        let expr = "@balance > 0";
        let parsed = parse(expr).unwrap();
        assert!(matches!(
            &parsed,
            Expression::Condition(Condition {
                left: ArithmeticExpr::Variable(ConditionLeft::Simple(
                    "balance",
                    VariableSource::Table
                )),
                ..
            })
        ));

        // $value should parse as an event reference
        let expr2 = "$value > 0";
        let parsed2 = parse(expr2).unwrap();
        assert!(matches!(
            &parsed2,
            Expression::Condition(Condition {
                left: ArithmeticExpr::Variable(ConditionLeft::Simple(
                    "value",
                    VariableSource::Event
                )),
                ..
            })
        ));

        // Mixed: event and table references
        let expr3 = "$value > @balance";
        let parsed3 = parse(expr3).unwrap();
        assert!(matches!(
            &parsed3,
            Expression::Condition(Condition {
                left: ArithmeticExpr::Variable(ConditionLeft::Simple(
                    "value",
                    VariableSource::Event
                )),
                right: ArithmeticExpr::Variable(ConditionLeft::Simple(
                    "balance",
                    VariableSource::Table
                )),
                ..
            })
        ));
    }

    #[test]
    fn test_has_table_references() {
        // Pure event condition - no table references
        let expr1 = parse("$value > 100").unwrap();
        assert!(!expr1.has_table_references());

        // Table reference on right side
        let expr2 = parse("$value > @balance").unwrap();
        assert!(expr2.has_table_references());

        // Table reference on left side
        let expr3 = parse("@balance < 1000").unwrap();
        assert!(expr3.has_table_references());

        // Logical expression with table reference
        let expr4 = parse("$value > 0 && $value > @balance").unwrap();
        assert!(expr4.has_table_references());

        // Logical expression without table reference
        let expr5 = parse("$value > 0 && $from != $to").unwrap();
        assert!(!expr5.has_table_references());
    }

    #[test]
    fn test_to_sql_condition_simple() {
        let table_name = "token_balances";

        // Event variable becomes EXCLUDED."column" (quoted for safety)
        let expr1 = parse("$value > 100").unwrap();
        assert_eq!(expr1.to_sql_condition(table_name), "EXCLUDED.\"value\" > 100");

        // Table variable becomes table_name."column"
        let expr2 = parse("@balance < 1000").unwrap();
        assert_eq!(expr2.to_sql_condition(table_name), "token_balances.\"balance\" < 1000");

        // Mixed: event > table
        let expr3 = parse("$value > @balance").unwrap();
        assert_eq!(
            expr3.to_sql_condition(table_name),
            "EXCLUDED.\"value\" > token_balances.\"balance\""
        );
    }

    #[test]
    fn test_to_sql_condition_logical() {
        let table_name = "token_balances";

        // AND condition
        let expr1 = parse("$value > 0 && $value > @balance").unwrap();
        assert_eq!(
            expr1.to_sql_condition(table_name),
            "(EXCLUDED.\"value\" > 0 AND EXCLUDED.\"value\" > token_balances.\"balance\")"
        );

        // OR condition
        let expr2 = parse("$value > @balance || $force == true").unwrap();
        assert_eq!(
            expr2.to_sql_condition(table_name),
            "(EXCLUDED.\"value\" > token_balances.\"balance\" OR EXCLUDED.\"force\" = TRUE)"
        );
    }

    #[test]
    fn test_to_sql_condition_with_schema() {
        // Table name with schema prefix
        let table_name = "\"myschema\".\"token_balances\"";

        let expr = parse("$value > @balance").unwrap();
        assert_eq!(
            expr.to_sql_condition(table_name),
            "EXCLUDED.\"value\" > \"myschema\".\"token_balances\".\"balance\""
        );
    }

    #[test]
    fn test_to_sql_condition_reserved_keywords() {
        // Test that reserved keywords like "order", "group" are safely quoted
        let table_name = "my_table";

        let expr = parse("$order > @group").unwrap();
        assert_eq!(expr.to_sql_condition(table_name), "EXCLUDED.\"order\" > my_table.\"group\"");
    }

    #[test]
    fn test_to_sql_condition_string_escaping() {
        // Test that single quotes in strings are properly escaped
        let table_name = "my_table";

        // String with single quote should be escaped
        let expr = parse("$name == \"O'Brien\"").unwrap();
        assert_eq!(expr.to_sql_condition(table_name), "EXCLUDED.\"name\" = 'O''Brien'");

        // Multiple quotes
        let expr2 = parse("$desc == \"It's a 'test'\"").unwrap();
        assert_eq!(expr2.to_sql_condition(table_name), "EXCLUDED.\"desc\" = 'It''s a ''test'''");
    }
}
