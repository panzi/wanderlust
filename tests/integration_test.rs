
mod helper;

const TEST_CREATE_BASIC_TABLE: &str = include_str!("001_create/001_basic_table.sql");
const TEST_CREATE_FOREIGN_KEY_AND_CONSTRAINTS: &str = include_str!("001_create/002_foreign_key_and_constraints.sql");
const TEST_CREATE_INDEX: &str = include_str!("001_create/003_index.sql");
const TEST_CREATE_FUNCTION: &str = include_str!("001_create/004_function.sql");
const TEST_CREATE_TRIGGER: &str = include_str!("001_create/005_trigger.sql");
const TEST_CREATE_TYPES: &str = include_str!("001_create/006_types.sql");
const TEST_CREATE_COMPLEX_TABLE: &str = include_str!("001_create/007_complex_table.sql");
const TEST_ALTER_ADD_COLUMN: &str = include_str!("002_alter/001_add_column.sql");
const TEST_ALTER_ALTER_COLUMN: &str = include_str!("002_alter/002_alter_column.sql");
const TEST_ALTER_ALTER_CONSTRAINTS: &str = include_str!("002_alter/003_alter_constraints.sql");
const TEST_ALTER_ADD_TO_TYPE: &str = include_str!("002_alter/004_add_to_type.sql");
const TEST_ALTER_REMOVE_FROM_TYPE: &str = include_str!("002_alter/005_remove_from_type.sql");
const TEST_ALTER_DROP_COLUMN: &str = include_str!("002_alter/006_drop_column.sql");
const TEST_ALTER_COMPLEX_TABLE: &str = include_str!("002_alter/007_complex_table.sql");

mod test_create {

    #[test]
    fn test_create_basic_table() {
        crate::helper::run_test("create", "basic_table", "tests/001_create/001_basic_table.sql", crate::TEST_CREATE_BASIC_TABLE);
    }

    #[test]
    fn test_create_foreign_key_and_constraints() {
        crate::helper::run_test("create", "foreign_key_and_constraints", "tests/001_create/002_foreign_key_and_constraints.sql", crate::TEST_CREATE_FOREIGN_KEY_AND_CONSTRAINTS);
    }

    #[test]
    fn test_create_index() {
        crate::helper::run_test("create", "index", "tests/001_create/003_index.sql", crate::TEST_CREATE_INDEX);
    }

    #[test]
    fn test_create_function() {
        crate::helper::run_test("create", "function", "tests/001_create/004_function.sql", crate::TEST_CREATE_FUNCTION);
    }

    #[test]
    fn test_create_trigger() {
        crate::helper::run_test("create", "trigger", "tests/001_create/005_trigger.sql", crate::TEST_CREATE_TRIGGER);
    }

    #[test]
    fn test_create_types() {
        crate::helper::run_test("create", "types", "tests/001_create/006_types.sql", crate::TEST_CREATE_TYPES);
    }

    #[test]
    fn test_create_complex_table() {
        crate::helper::run_test("create", "complex_table", "tests/001_create/007_complex_table.sql", crate::TEST_CREATE_COMPLEX_TABLE);
    }
}

mod test_alter {

    #[test]
    fn test_alter_add_column() {
        crate::helper::run_test("alter", "add_column", "tests/002_alter/001_add_column.sql", crate::TEST_ALTER_ADD_COLUMN);
    }

    #[test]
    fn test_alter_alter_column() {
        crate::helper::run_test("alter", "alter_column", "tests/002_alter/002_alter_column.sql", crate::TEST_ALTER_ALTER_COLUMN);
    }

    #[test]
    fn test_alter_alter_constraints() {
        crate::helper::run_test("alter", "alter_constraints", "tests/002_alter/003_alter_constraints.sql", crate::TEST_ALTER_ALTER_CONSTRAINTS);
    }

    #[test]
    fn test_alter_add_to_type() {
        crate::helper::run_test("alter", "add_to_type", "tests/002_alter/004_add_to_type.sql", crate::TEST_ALTER_ADD_TO_TYPE);
    }

    #[test]
    fn test_alter_remove_from_type() {
        crate::helper::run_test("alter", "remove_from_type", "tests/002_alter/005_remove_from_type.sql", crate::TEST_ALTER_REMOVE_FROM_TYPE);
    }

    #[test]
    fn test_alter_drop_column() {
        crate::helper::run_test("alter", "drop_column", "tests/002_alter/006_drop_column.sql", crate::TEST_ALTER_DROP_COLUMN);
    }

    #[test]
    fn test_alter_complex_table() {
        crate::helper::run_test("alter", "complex_table", "tests/002_alter/007_complex_table.sql", crate::TEST_ALTER_COMPLEX_TABLE);
    }
}

mod test_drop {
}
