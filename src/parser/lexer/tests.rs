use super::*;

#[test]
fn spec() {
	let cases = [
		empty(),
		simple_tags(),
		quote_inside_tag_err(),
		quote_inside_tag_ok(),
		many_quotes_ok(),
		many_quotes_err(),
		cyrillic(),
		invisible_operators(),
		operators_in_quotes(),
	];
	for (query, expected) in cases {
		assert_eq!(tokenize(query), expected);
	}
}

type Test = (&'static str, Result<'static, Vec<Token<'static>>>);

fn empty() -> Test {
	("", Ok(vec![]))
}

fn simple_tags() -> Test {
	(
		"Ph'nglui fhtagn",
		Ok(vec![
			Token::Tag {
				value: "Ph'nglui",
				start: 0,
				end: 8,
			},
			Token::Tag {
				value: "fhtagn",
				start: 9,
				end: 15,
			},
		]),
	)
}

fn quote_inside_tag_err() -> Test {
	(
		"kek\"no... \"",
		Err(ParseError::UnpairedQuote { position: 10 }),
	)
}

fn quote_inside_tag_ok() -> Test {
	(
		"o\"oops \"\"",
		Ok(vec![
			Token::Tag {
				value: "o\"oops",
				start: 0,
				end: 6,
			},
			Token::Tag {
				value: "",
				start: 7,
				end: 9,
			},
		]),
	)
}

fn many_quotes_ok() -> Test {
	(
		"\"\"\"\"\"\"",
		Ok(vec![
			Token::Tag {
				value: "",
				start: 0,
				end: 2,
			},
			Token::Tag {
				value: "",
				start: 2,
				end: 4,
			},
			Token::Tag {
				value: "",
				start: 4,
				end: 6,
			},
		]),
	)
}

fn many_quotes_err() -> Test {
	(
		"\"\"\"\"\"\"\"",
		Err(ParseError::UnpairedQuote { position: 6 }),
	)
}

fn cyrillic() -> Test {
	(
		"Опа, кириллица. or else?...",
		Ok(vec![
			Token::Tag {
				value: "Опа,",
				start: 0,
				end: 7,
			},
			Token::Tag {
				value: "кириллица.",
				start: 8,
				end: 27,
			},
			Token::Operator {
				kind: OperatorKind::Or,
				position: 28,
			},
			Token::Tag {
				value: "else?...",
				start: 31,
				end: 39,
			},
		]),
	)
}

fn invisible_operators() -> Test {
	(
		"or;and;not)or_gibberish   (         ",
		Ok(vec![
			Token::Operator {
				kind: OperatorKind::Or,
				position: 0,
			},
			Token::Tag {
				value: ";and;not",
				start: 2,
				end: 10,
			},
			Token::Operator {
				kind: OperatorKind::CloseParen,
				position: 10,
			},
			Token::Operator {
				kind: OperatorKind::Or,
				position: 11,
			},
			Token::Tag {
				value: "_gibberish",
				start: 13,
				end: 23,
			},
			Token::Operator {
				kind: OperatorKind::OpenParen,
				position: 26,
			},
		]),
	)
}

fn operators_in_quotes() -> Test {
	(
		"and~ wow   oR NOT such~ \"a disappointment, not\" ornot?",
		Ok(vec![
			Token::Operator {
				kind: OperatorKind::And,
				position: 0,
			},
			Token::Operator {
				kind: OperatorKind::Fuzzy,
				position: 3,
			},
			Token::Tag {
				value: "wow",
				start: 5,
				end: 8,
			},
			Token::Operator {
				kind: OperatorKind::Or,
				position: 11,
			},
			Token::Operator {
				kind: OperatorKind::Not,
				position: 14,
			},
			Token::Tag {
				value: "such",
				start: 18,
				end: 22,
			},
			Token::Operator {
				kind: OperatorKind::Fuzzy,
				position: 22,
			},
			Token::Tag {
				value: "a disappointment, not",
				start: 24,
				end: 47,
			},
			Token::Tag {
				value: "ornot?",
				start: 48,
				end: 54,
			},
		]),
	)
}
