// use pest::Parser;

#[derive(Parser)]
#[grammar = "parser/grammar.pest"]
pub struct SQLParser;
