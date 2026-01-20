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

/// Represents the possible arithmetic operators that can be used in filter expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithmeticOperator {
    /// Addition operator (+)
    Add,
    /// Subtraction operator (-)
    Subtract,
    /// Multiplication operator (*)
    Multiply,
    /// Division operator (/)
    Divide,
    /// Exponentiation operator (^)
    Power,
}

/// Represents the possible logical operators that can be used in filter expressions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalOperator {
    /// Logical AND operator (&&)
    And,
    /// Logical OR operator (||)
    Or,
}

/// Represents the source of a variable reference in a filter expression.
/// This distinguishes between event data and table (database) state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VariableSource {
    /// Variable comes from event data ($var or bare var)
    #[default]
    Event,
    /// Variable comes from current table row state (@var)
    Table,
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
    /// The source of this variable (event data or table state)
    #[allow(dead_code)]
    pub source: VariableSource,
}

/// Represents the left side of a condition (LHS) in a filter expression.
/// The left side can either be a simple variable name or a path to a variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionLeft<'a> {
    /// A simple variable name (e.g., "name", "age", etc.)
    /// This is a direct reference to a variable in the data structure.
    /// The second field indicates the source (Event for $var, Table for @var).
    Simple(&'a str, VariableSource),
    /// A sequence of accessors that form a path to a variable (e.g., "person.name", "person[0].age", etc.)
    Path(VariablePath<'a>),
}

impl<'a> ConditionLeft<'a> {
    /// Helper method get the base name of the variable or path.
    pub fn base_name(&self) -> &'a str {
        match self {
            ConditionLeft::Simple(name, _) => name,
            ConditionLeft::Path(path) => path.base,
        }
    }

    /// Helper method to get the accessors of the variable path.
    /// If ConditionLeft is a simple variable, it returns an empty slice.
    /// If it is a path, it returns the accessors of that path.
    /// Used during evaluation to traverse nested structures.
    pub fn accessors(&self) -> &[Accessor<'_>] {
        match self {
            ConditionLeft::Simple(_, _) => &[],
            ConditionLeft::Path(path) => &path.accessors,
        }
    }

    /// Helper method to get the source of the variable (Event or Table).
    pub fn source(&self) -> VariableSource {
        match self {
            ConditionLeft::Simple(_, source) => *source,
            ConditionLeft::Path(path) => path.source,
        }
    }
}

/// Represents an arithmetic expression that can be used in filter conditions.
/// Supports basic arithmetic operations (+, -, *, /) on variables and literals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArithmeticExpr<'a> {
    /// A variable reference (event field)
    Variable(ConditionLeft<'a>),
    /// A literal value (number, string, etc.)
    Literal(LiteralValue<'a>),
    /// A binary arithmetic operation
    Binary {
        /// The left operand
        left: Box<ArithmeticExpr<'a>>,
        /// The arithmetic operator (+, -, *, /)
        operator: ArithmeticOperator,
        /// The right operand
        right: Box<ArithmeticExpr<'a>>,
    },
}

/// Represents a condition in a filter expression.
/// A condition consists of a left side (LHS), an operator, and a right side (RHS).
/// Both sides can be arithmetic expressions (e.g., "value + fee > balance * 2").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Condition<'a> {
    /// The left side of the condition (LHS).
    /// Can be a variable, literal, or arithmetic expression.
    pub left: ArithmeticExpr<'a>,
    /// The operator used in the condition (e.g., ==, !=, >, <, etc.)
    pub operator: ComparisonOperator,
    /// The right side of the condition (RHS).
    /// Can be a variable, literal, or arithmetic expression.
    pub right: ArithmeticExpr<'a>,
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
    /// A negated expression (e.g., "!($value > 100)" or "!($a == 1 && $b == 2)")
    Not(Box<Expression<'a>>),
}

impl<'a> Expression<'a> {
    /// Checks if this expression contains any table references (@variables).
    /// Returns true if any variable in the expression has VariableSource::Table.
    pub fn has_table_references(&self) -> bool {
        match self {
            Expression::Condition(cond) => {
                cond.left.has_table_references() || cond.right.has_table_references()
            }
            Expression::Logical { left, right, .. } => {
                left.has_table_references() || right.has_table_references()
            }
            Expression::Not(inner) => inner.has_table_references(),
        }
    }

    /// Converts this expression to a SQL WHERE clause string.
    /// - Event variables ($var) become EXCLUDED.var (the new values being inserted)
    /// - Table variables (@var) become {table_name}.var (current DB values)
    pub fn to_sql_condition(&self, table_name: &str) -> String {
        match self {
            Expression::Condition(cond) => {
                let left_sql = cond.left.to_sql(table_name);
                let right_sql = cond.right.to_sql(table_name);
                let op_sql = match cond.operator {
                    ComparisonOperator::Eq => "=",
                    ComparisonOperator::Ne => "<>",
                    ComparisonOperator::Gt => ">",
                    ComparisonOperator::Gte => ">=",
                    ComparisonOperator::Lt => "<",
                    ComparisonOperator::Lte => "<=",
                };
                format!("{} {} {}", left_sql, op_sql, right_sql)
            }
            Expression::Logical { left, operator, right } => {
                let left_sql = left.to_sql_condition(table_name);
                let right_sql = right.to_sql_condition(table_name);
                let op_sql = match operator {
                    LogicalOperator::And => "AND",
                    LogicalOperator::Or => "OR",
                };
                format!("({} {} {})", left_sql, op_sql, right_sql)
            }
            Expression::Not(inner) => {
                format!("NOT ({})", inner.to_sql_condition(table_name))
            }
        }
    }
}

impl<'a> ArithmeticExpr<'a> {
    /// Checks if this arithmetic expression contains any table references.
    pub fn has_table_references(&self) -> bool {
        match self {
            ArithmeticExpr::Variable(cond_left) => cond_left.source() == VariableSource::Table,
            ArithmeticExpr::Literal(_) => false,
            ArithmeticExpr::Binary { left, right, .. } => {
                left.has_table_references() || right.has_table_references()
            }
        }
    }

    /// Converts this arithmetic expression to SQL.
    /// Column names are double-quoted to handle reserved keywords safely.
    pub fn to_sql(&self, table_name: &str) -> String {
        match self {
            ArithmeticExpr::Variable(cond_left) => {
                let col_name = cond_left.base_name();
                match cond_left.source() {
                    VariableSource::Event => format!("EXCLUDED.\"{}\"", col_name),
                    VariableSource::Table => format!("{}.\"{}\"", table_name, col_name),
                }
            }
            ArithmeticExpr::Literal(lit) => match lit {
                LiteralValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
                LiteralValue::Str(s) => format!("'{}'", s.replace('\'', "''")),
                LiteralValue::Number(n) => n.to_string(),
            },
            ArithmeticExpr::Binary { left, operator, right } => {
                let left_sql = left.to_sql(table_name);
                let right_sql = right.to_sql(table_name);
                match operator {
                    ArithmeticOperator::Power => {
                        // Use PostgreSQL POWER() function for exponentiation
                        format!("POWER({}, {})", left_sql, right_sql)
                    }
                    _ => {
                        let op_sql = match operator {
                            ArithmeticOperator::Add => "+",
                            ArithmeticOperator::Subtract => "-",
                            ArithmeticOperator::Multiply => "*",
                            ArithmeticOperator::Divide => "/",
                            ArithmeticOperator::Power => unreachable!(),
                        };
                        format!("({} {} {})", left_sql, op_sql, right_sql)
                    }
                }
            }
        }
    }
}
