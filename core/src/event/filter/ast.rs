//! This module defines the abstract syntax tree (AST) for the filter expressions.
//! Parsing module will convert the input string into this AST structure.
//! This AST is then traversed and interpreted by the evaluation module to determine the result of the filter expression.
//!
//! The AST is designed to be a direct representation of the parsed filter expression, capturing it's structure, operators and literal values.
//! Lifetime annotations (`'a`) are used to ensure that the references to string literals are valid for the duration of the expression evaluation.

/// Represents the possible literal values that can be used in filter expressions.
/// The `LiteralValue` enum captures the different constant values that are used on the right side of a condition (RHS).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiteralValue<'a> {
    /// A boolean literal value.
    Bool(bool),
    /// A string literal value. Includes both single-quoted and unquoted strings, includes hexadecimal strings.
    /// e.g., "abc", 'abc', '0x123ABC'
    Str(&'a str),
    /// A numeric literal value. e.g., "123", "-123.456", "0x123" or hexadecimal
    /// Store as string slice to preserve original form until evaluation phase.
    /// Conversion to specific type is done within chain context during evaluation.
    Number(&'a str),
}

/// Represents the possible comparison operators that can be used in filter expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOperator {
    /// Equality operator (==)
    Eq,
    /// Inequality operator (!=)
    Ne,
    /// Greater than operator (>)
    Gt,
    /// Greater than or equal to operator (>=)
    Gte,
    /// Less than operator (<)
    Lt,
    /// Less than or equal to operator (<=)
    Lte,
}

/// Represents the possible logical operators that can be used in filter expressions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalOperator {
    /// Logical AND operator (&&)
    And,
    /// Logical OR operator (||)
    Or,
}

/// Represents the possible accessors that can be used in filter expressions.
/// Accessors are used to access elements in collections or properties in objects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Accessor<'a> {
    /// Accessor for a collection index (e.g., [0], [1], etc.)
    Index(usize),
    /// Accessor for a property name (e.g., .name, .age, etc.)
    Key(&'a str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VariablePath<'a> {
    pub base: &'a str,
    pub accessors: Vec<Accessor<'a>>,
}

/// Represents the left side of a condition (LHS) in a filter expression.
/// The left side can either be a simple variable name or a path to a variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionLeft<'a> {
    /// A simple variable name (e.g., "name", "age", etc.)
    /// This is a direct reference to a variable in the data structure.
    Simple(&'a str),
    /// A sequence of accessors that form a path to a variable (e.g., "person.name", "person[0].age", etc.)
    Path(VariablePath<'a>),
}

impl<'a> ConditionLeft<'a> {
    /// Helper method get the base name of the variable or path.
    pub fn base_name(&self) -> &'a str {
        match self {
            ConditionLeft::Simple(name) => name,
            ConditionLeft::Path(path) => path.base,
        }
    }

    /// Helper method to get the accessors of the variable path.
    /// If ConditionLeft is a simple variable, it returns an empty slice.
    /// If it is a path, it returns the accessors of that path.
    /// Used during evaluation to traverse nested structures.
    pub fn accessors(&self) -> &[Accessor] {
        match self {
            ConditionLeft::Simple(_) => &[],
            ConditionLeft::Path(path) => &path.accessors,
        }
    }
}

/// Represents a condition in a filter expression.
/// A condition consists of a left side (LHS), an operator, and a right side (RHS).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Condition<'a> {
    /// The left side of the condition (LHS).
    /// This can be a simple variable name or a path to a variable.
    pub left: ConditionLeft<'a>,
    /// The operator used in the condition (e.g., ==, !=, >, <, etc.)
    pub operator: ComparisonOperator,
    /// The right side of the condition (RHS).
    pub right: LiteralValue<'a>,
}

/// Represents a complete filter expression.
/// An expression can be a single condition or a logical combination of multiple conditions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expression<'a> {
    /// A simple condition (e.g., "age > 30")
    Condition(Condition<'a>),
    /// A logical combination of two expressions (e.g., "age > 30 && name == 'John'")
    /// `Box` is used to avoid infinite type recursion, as `Expression` can contain other `Expression`s.
    Logical {
        /// The left side sub-expression.
        left: Box<Expression<'a>>,
        /// The logical operator used to combine the two expressions: AND or OR.
        operator: LogicalOperator,
        /// The right side sub-expression.
        right: Box<Expression<'a>>,
    },
}
