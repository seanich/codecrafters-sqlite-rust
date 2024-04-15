extern crate peg;

#[derive(Debug, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Create(CreateStatement),
}

#[derive(Debug, PartialEq)]
pub struct CreateStatement {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, PartialEq)]
pub struct SelectStatement {
    pub select: Vec<String>,
    pub from: String,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, PartialEq)]
pub struct WhereClause {
    pub column: String,
    pub value: String,
}

peg::parser! {
    pub grammar sql() for str {
        pub rule sql_statement() -> Statement
        = _ s:(select_statement() / create_table_statement()) _ { s }

        rule select_statement() -> Statement
        = i("SELECT") _ fields:(select() ++ ("," _)) _ i("FROM") _ from:ident() _ w:(where_clause())? {
            Statement::Select(SelectStatement {
                select: fields,
                from,
                where_clause: w,
            })
        }

        rule where_clause() -> WhereClause
        = i("WHERE") _ column:(ident()) _ "=" _ "'" value:$([^'\'']*) "'" {
            WhereClause {
                column,
                value: String::from(value),
            }
        }

        rule create_table_statement() -> Statement
        = i("CREATE") _ i("TABLE") _ name:(ident()) _ "(" _ c:(column() ++ ("," _)) _ ")"  {
            Statement::Create(CreateStatement {
                name,
                columns: c.into_iter().collect()
            })
        }

        rule select() -> String = s:(i("COUNT(*)") / ident()) { s }

        rule column() -> String = n:(ident()) _ ident() (_ ident())* { n }

        rule ident() -> String
        = chars:$(alpha() [ '_' | '0'..='9']*) { chars.to_string() }

        rule alpha() -> String
        = chars:$(['a'..='z' | 'A'..='Z']+) { chars.to_string() }

        rule i(literal: &'static str) -> String
        = input:$([_]*<{literal.len()}>)
          {? if input.eq_ignore_ascii_case(literal) { Ok(literal.to_string()) } else { Err(literal) } }

        rule _ = [' ' | '\t' | '\n']*
    }
}

#[test]
fn select() {
    let statement = r#"
    SELECT
        id,
        name
    FROM foobar
    "#;

    assert_eq!(
        sql::sql_statement(statement),
        Ok(Statement::Select(SelectStatement {
            from: String::from("foobar"),
            select: vec![String::from("id"), String::from("name")],
            where_clause: None,
        }))
    )
}

#[test]
fn select_count() {
    let statement = r#"
    SELECT
        COUNT(*)
    FROM foobar
    "#;

    assert_eq!(
        sql::sql_statement(statement),
        Ok(Statement::Select(SelectStatement {
            from: String::from("foobar"),
            select: vec![String::from("COUNT(*)")],
            where_clause: None,
        }))
    )
}

#[test]
fn select_with_where() {
    let statement = r#"
    SELECT
        id,
        name
    FROM foobar
    WHERE name = 'Some Guy'
    "#;

    assert_eq!(
        sql::sql_statement(statement),
        Ok(Statement::Select(SelectStatement {
            from: String::from("foobar"),
            select: vec![String::from("id"), String::from("name")],
            where_clause: Some(WhereClause {
                column: String::from("name"),
                value: String::from("Some Guy"),
            })
        }))
    )
}

#[test]
fn create_table() {
    let statement = r#"
    CReaTE TABLE foobar (
        id integer autoincrement,
        name varchar
    )
    "#;

    assert_eq!(
        sql::sql_statement(statement),
        Ok(Statement::Create(CreateStatement {
            name: String::from("foobar"),
            columns: vec![String::from("id"), String::from("name")]
        }))
    )
}
