use super::data::*;

pub struct Tokenizer<'a> {
	tail: &'a str,
	position: usize,
}

impl<'a> From<&'a str> for Tokenizer<'a> {
	fn from(input: &'a str) -> Self {
		Self {
			tail: input,
			position: 0,
		}
	}
}

impl<'a> Iterator for Tokenizer<'a> {
	type Item = Result<'a, Token<'a>>;

	fn next(&mut self) -> Option<Self::Item> {
		self.skip_whitespace();
		if self.tail.is_empty() {
			return None;
		}
		let position = self.position;
		let result = if let Some(kind) = self.next_operator() {
			Ok(Token::Operator { kind, position })
		} else {
			self.next_tag().map(|value| {
				let (start, end) = (position, self.position);
				Token::Tag { value, start, end }
			})
		};
		Some(result)
	}
}

impl<'a> Tokenizer<'a> {
	fn skip_whitespace(&mut self) {
		self.advance_bytes(self.try_chars(|c| c.is_whitespace()));
	}

	fn next_operator(&mut self) -> Option<OperatorKind> {
		{
			let c = self.tail.bytes().next()?;
			let dict = [
				(b'(', OperatorKind::OpenParen),
				(b')', OperatorKind::CloseParen),
				(b'~', OperatorKind::Fuzzy),
			];
			for (op, kind) in dict {
				if c == op {
					self.advance_bytes(1);
					return Some(kind);
				}
			}
		}
		{
			let count = self.try_bytes(u8::is_ascii_alphabetic);
			let dict = [
				("and", OperatorKind::And),
				("or", OperatorKind::Or),
				("not", OperatorKind::Not),
			];
			for (sample, kind) in dict {
				if sample.eq_ignore_ascii_case(&self.tail[..count]) {
					self.advance_bytes(count);
					return Some(kind);
				}
			}
		}
		None
	}

	fn next_tag(&mut self) -> Result<'a, &'a str> {
		let c = self.tail.bytes().next().unwrap();
		if c == b'"' {
			let position = self.position;
			self.advance_bytes(1);
			let phrase = self.try_chars(|&c| c != '"');
			if phrase == self.tail.len() {
				Err(ParseError::UnpairedQuote { position })
			} else {
				let result = self.advance_bytes(phrase);
				self.advance_bytes(1);
				Ok(result)
			}
		} else {
			Ok(self.advance_bytes(self.try_chars(|&c| !"~()".contains(c) && !c.is_whitespace())))
		}
	}

	fn try_bytes(&self, f: impl FnMut(&u8) -> bool) -> usize {
		self.tail.bytes().take_while(f).count()
	}

	fn try_chars(&self, f: impl FnMut(&char) -> bool) -> usize {
		self.tail.chars().take_while(f).map(char::len_utf8).sum()
	}

	fn advance_bytes(&mut self, count: usize) -> &'a str {
		let (word, tail) = self.tail.split_at(count);
		self.position += count;
		self.tail = tail;
		word
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn tokenize(query: &str) -> Result<Vec<Token>> {
		Tokenizer::from(query).collect()
	}

	#[test]
	fn spec() {
		let cases = [
			("", Ok(vec![])),
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
			),
			(
				"kek\"no... \"",
				Err(ParseError::UnpairedQuote { position: 10 }),
			),
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
			),
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
			),
			(
				"\"\"\"\"\"\"\"",
				Err(ParseError::UnpairedQuote { position: 6 }),
			),
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
			),
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
			),
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
			),
		];
		for (query, expected) in cases {
			assert_eq!(tokenize(query), expected);
		}
	}
}
