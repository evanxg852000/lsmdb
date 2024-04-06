#[macro_use]
extern crate pest_derive;

mod parser;


#[cfg(test)]
mod tests {
    use anyhow::Ok;

    use crate::parser::{SQLParser, Rule};
    use pest::Parser;

    #[test]
    fn test_sql_parser() -> anyhow::Result<()> {
        let statement = "select 1 from test;";
        let parse_tree = SQLParser::parse(Rule::http, statement);
        

        println!("Tree: \n {:?} \n", parse_tree);
        
        Ok(())
    }

}
