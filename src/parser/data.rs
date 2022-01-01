pub type Result<'a, T> = std::result::Result<T, ParseError<'a>>;

#[derive(Debug, PartialEq, Eq)]
pub enum ParseError<'a> {
	UnpairedQuote {
		position: usize,
	},
	#[allow(dead_code)]
	MissingOperator {
		expected: OperatorKind,
		token: Token<'a>,
	},
}

#[derive(Debug, PartialEq, Eq)]
pub enum Token<'a> {
	Tag {
		value: &'a str,
		start: usize,
		end: usize,
	},
	Operator {
		kind: OperatorKind,
		position: usize,
	},
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperatorKind {
	OpenParen,
	CloseParen,
	Fuzzy,
	Not,
	And,
	Or,
}
