#[cfg(test)]
mod tests;

use super::data::*;

pub fn tokenize(query: &str) -> Result<Vec<Token>> {
	Tokenizer::from(query).collect()
}

struct Tokenizer<'a> {
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
		self.next_symbolic_operator()
			.or_else(|| self.next_worded_operator())
			.map(|kind| Ok(Token::Operator { kind, position }))
			.or_else(|| Some(self.next_tag(position)))
	}
}

impl<'a> Tokenizer<'a> {
	fn skip_whitespace(&mut self) {
		self.advance_bytes(self.try_chars(char::is_whitespace));
	}

	fn next_symbolic_operator(&mut self) -> Option<OperatorKind> {
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
		None
	}

	fn next_worded_operator(&mut self) -> Option<OperatorKind> {
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
		None
	}

	fn next_tag(&mut self, start: usize) -> Result<'a, Token<'a>> {
		self.next_tag_value().map(|value| Token::Tag {
			value,
			start,
			end: self.position,
		})
	}

	fn next_tag_value(&mut self) -> Result<'a, &'a str> {
		let c = self.tail.bytes().next().unwrap();
		if c == b'"' {
			let position = self.position;
			self.advance_bytes(1);
			let phrase_len = self.try_chars(|c| c != '"');
			if phrase_len == self.tail.len() {
				Err(ParseError::UnpairedQuote { position })
			} else {
				let result = self.advance_bytes(phrase_len);
				self.advance_bytes(1);
				Ok(result)
			}
		} else {
			let word_char = |c: char| !"~()".contains(c) && !c.is_whitespace();
			Ok(self.advance_bytes(self.try_chars(word_char)))
		}
	}

	fn try_bytes(&self, f: impl FnMut(&u8) -> bool) -> usize {
		self.tail.bytes().take_while(f).count()
	}

	fn try_chars(&self, mut f: impl FnMut(char) -> bool) -> usize {
		self.tail
			.chars()
			.take_while(|&c| f(c))
			.map(char::len_utf8)
			.sum()
	}

	fn advance_bytes(&mut self, count: usize) -> &'a str {
		let (word, tail) = self.tail.split_at(count);
		self.position += count;
		self.tail = tail;
		word
	}
}
