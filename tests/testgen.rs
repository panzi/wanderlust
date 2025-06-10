use std::{collections::HashMap, io::BufWriter, path::PathBuf};
use std::io::Write;

#[derive(Debug)]
struct Test {
    number: u32,
    name: String,
    path: PathBuf,
}

impl PartialOrd for Test {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Test {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.number == other.number
    }
}

impl Ord for Test {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.number.cmp(&other.number)
    }
}

impl Eq for Test {
}

#[derive(Debug)]
struct TestSuit {
    number: u32,
    name: String,
    tests: Vec<Test>,
}

impl PartialOrd for TestSuit {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TestSuit {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.number == other.number
    }
}

impl Ord for TestSuit {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.number.cmp(&other.number)
    }
}

impl Eq for TestSuit {
}

fn main() -> std::io::Result<()> {
    // TODO: generate tests
    // * find all tests/**/*.sql files
    // * split them in before and after
    // * generate migrations between those
    // * generate tests for each sql file that
    //   * runs "before" against postgres
    //   * runs the migrations
    //   * dumps the new schema
    //   * compares the schema against "after"

    let mut test_suits = HashMap::new();

    for suite_entry in std::fs::read_dir("tests")? {
        let suite_entry = suite_entry?;
        if !suite_entry.metadata()?.is_dir() {
            continue;
        }
        let Ok(suite_filename) = suite_entry.file_name().into_string() else {
            continue;
        };
        let Some(index) = suite_filename.find('_') else {
            continue;
        };

        let Ok(suite_nr) = suite_filename[..index].parse::<u32>() else {
            continue;
        };
        let suite_name = &suite_filename[index + 1..];
        let mut tests = HashMap::new();

        for test_entry in std::fs::read_dir(suite_entry.path())? {
            let test_entry = test_entry?;
            if !test_entry.metadata()?.is_file() {
                continue;
            }

            let Ok(test_filename) = test_entry.file_name().into_string() else {
                continue;
            };

            if test_filename.len() <= 4 || !test_filename[test_filename.len() - 4..].eq_ignore_ascii_case(".sql") {
                continue;
            }

            let test_name = &test_filename[..test_filename.len() - 4];
            let Some(index) = test_name.find('_') else {
                continue;
            };
            let Ok(test_nr) = test_name[..index].parse() else {
                continue;
            };
            let test_name = &test_name[index + 1..];

            if tests.insert(test_nr, Test {
                number: test_nr,
                name: test_name.to_string(),
                path: test_entry.path()
            }).is_some() {
                panic!("{suite_nr:03} {suite_name}: duplicated test number: {test_nr:03}");
            }
        }

        let mut tests: Vec<_> = tests.into_values().collect();
        tests.sort();

        if test_suits.insert(suite_nr, TestSuit {
            number: suite_nr,
            name: suite_name.to_string(),
            tests
        }).is_some() {
            panic!("duplicates test suit number: {suite_nr:03}");
        }
    }

    let mut test_suits: Vec<_> = test_suits.into_values().collect();
    test_suits.sort();

    let integration_test_path: PathBuf = ["tests", "integration_test.rs"].iter().collect();
    let mut out = BufWriter::new(std::fs::File::options().create(true).write(true).truncate(true).open(integration_test_path)?);

    writeln!(out, "
mod helper;
")?;

    for test_suite in &test_suits {
        let test_suite_upper = test_suite.name.to_uppercase();
        for test in &test_suite.tests {
            let rel_path: PathBuf = test.path.components().skip(1).collect();
            writeln!(out,
                "const TEST_{}_{}: &str = include_str!({:?});",
                test_suite_upper,
                test.name.to_uppercase(),
                rel_path
            )?;
        }
    }

    for test_suite in &test_suits {
        let test_suite_upper = test_suite.name.to_uppercase();
        writeln!(out, "
mod test_{} {{", test_suite.name)?;
        for test in &test_suite.tests {
            let test_upper = test.name.to_uppercase();
            write!(out, "
    #[test]
    fn test_{}_{}() {{
        let test_data = crate::helper::load_file({:?}, crate::TEST_{}_{});
        let migrations = wanderlust::migration::generate_migration(&test_data.before, &test_data.after);
        // TODO: run against PostgreSQL and compare result
    }}
",
                test_suite.name, test.name, test.path, test_suite_upper, test_upper,
            )?;
        }
        writeln!(out, "}}")?;
    }

    Ok(())
}
