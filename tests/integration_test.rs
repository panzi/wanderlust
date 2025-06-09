
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
        let test_data = crate::helper::load_file("tests/001_create/001_basic_table.sql", crate::TEST_CREATE_BASIC_TABLE);
        let migrations = wanderlust::migration::generate_migration(&test_data.before, &test_data.after);
        // TODO: run against PostgreSQL and compare result
    }

    #[test]
    fn test_create_foreign_key_and_constraints() {
        let test_data = crate::helper::load_file("tests/001_create/002_foreign_key_and_constraints.sql", crate::TEST_CREATE_FOREIGN_KEY_AND_CONSTRAINTS);
        let migrations = wanderlust::migration::generate_migration(&test_data.before, &test_data.after);
        // TODO: run against PostgreSQL and compare result
    }

    #[test]
    fn test_create_index() {
        let test_data = crate::helper::load_file("tests/001_create/003_index.sql", crate::TEST_CREATE_INDEX);
        let migrations = wanderlust::migration::generate_migration(&test_data.before, &test_data.after);
        // TODO: run against PostgreSQL and compare result
    }

    #[test]
    fn test_create_function() {
        let test_data = crate::helper::load_file("tests/001_create/004_function.sql", crate::TEST_CREATE_FUNCTION);
        let migrations = wanderlust::migration::generate_migration(&test_data.before, &test_data.after);
        // TODO: run against PostgreSQL and compare result
    }

    #[test]
    fn test_create_trigger() {
        let test_data = crate::helper::load_file("tests/001_create/005_trigger.sql", crate::TEST_CREATE_TRIGGER);
        let migrations = wanderlust::migration::generate_migration(&test_data.before, &test_data.after);
        // TODO: run against PostgreSQL and compare result
    }

    #[test]
    fn test_create_types() {
        let test_data = crate::helper::load_file("tests/001_create/006_types.sql", crate::TEST_CREATE_TYPES);
        let migrations = wanderlust::migration::generate_migration(&test_data.before, &test_data.after);
        // TODO: run against PostgreSQL and compare result
    }

    #[test]
    fn test_create_complex_table() {
        let test_data = crate::helper::load_file("tests/001_create/007_complex_table.sql", crate::TEST_CREATE_COMPLEX_TABLE);
        let migrations = wanderlust::migration::generate_migration(&test_data.before, &test_data.after);
        // TODO: run against PostgreSQL and compare result
    }
}

mod test_alter {

    #[test]
    fn test_alter_add_column() {
        let test_data = crate::helper::load_file("tests/002_alter/001_add_column.sql", crate::TEST_ALTER_ADD_COLUMN);
        let migrations = wanderlust::migration::generate_migration(&test_data.before, &test_data.after);
        // TODO: run against PostgreSQL and compare result
    }

    #[test]
    fn test_alter_alter_column() {
        let test_data = crate::helper::load_file("tests/002_alter/002_alter_column.sql", crate::TEST_ALTER_ALTER_COLUMN);
        let migrations = wanderlust::migration::generate_migration(&test_data.before, &test_data.after);
        // TODO: run against PostgreSQL and compare result
    }

    #[test]
    fn test_alter_alter_constraints() {
        let test_data = crate::helper::load_file("tests/002_alter/003_alter_constraints.sql", crate::TEST_ALTER_ALTER_CONSTRAINTS);
        let migrations = wanderlust::migration::generate_migration(&test_data.before, &test_data.after);
        // TODO: run against PostgreSQL and compare result
    }

    #[test]
    fn test_alter_add_to_type() {
        let test_data = crate::helper::load_file("tests/002_alter/004_add_to_type.sql", crate::TEST_ALTER_ADD_TO_TYPE);
        let migrations = wanderlust::migration::generate_migration(&test_data.before, &test_data.after);
        // TODO: run against PostgreSQL and compare result
    }

    #[test]
    fn test_alter_remove_from_type() {
        let test_data = crate::helper::load_file("tests/002_alter/005_remove_from_type.sql", crate::TEST_ALTER_REMOVE_FROM_TYPE);
        let migrations = wanderlust::migration::generate_migration(&test_data.before, &test_data.after);
        // TODO: run against PostgreSQL and compare result
    }

    #[test]
    fn test_alter_drop_column() {
        let test_data = crate::helper::load_file("tests/002_alter/006_drop_column.sql", crate::TEST_ALTER_DROP_COLUMN);
        let migrations = wanderlust::migration::generate_migration(&test_data.before, &test_data.after);
        // TODO: run against PostgreSQL and compare result
    }

    #[test]
    fn test_alter_complex_table() {
        let test_data = crate::helper::load_file("tests/002_alter/007_complex_table.sql", crate::TEST_ALTER_COMPLEX_TABLE);
        let migrations = wanderlust::migration::generate_migration(&test_data.before, &test_data.after);
        // TODO: run against PostgreSQL and compare result
    }
}

mod test_drop {
}
