mod data;
mod lexer;
mod pratt;

#[allow(dead_code)]
pub fn parse(query: &str) -> data::Result<Query> {
	Query::try_from(lexer::tokenize(query)?)
}

#[allow(dead_code)]
pub enum Query<'a> {
	Tag {
		value: &'a str,
		search_kind: SearchKind,
	},
	Not(Box<Query<'a>>),
	And(Box<Query<'a>>, Box<Query<'a>>),
	Or(Box<Query<'a>>, Box<Query<'a>>),
}

#[allow(dead_code)]
pub enum SearchKind {
	Exact,
	Fuzzy,
}

impl<'a> TryFrom<Vec<data::Token<'a>>> for Query<'a> {
	type Error = data::ParseError<'a>;

	fn try_from(value: Vec<data::Token<'a>>) -> Result<Self, Self::Error> {
		pratt::parse(value.into_iter())
	}
}
