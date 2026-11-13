use super::*;

fn check(expression: &str, alias: &str, field: &str, value: &str) -> ConditionCheck {
    ConditionCheck::builder()
        .table_name("arn:aws:dynamodb:us-west-2:123456789012:table/coord")
        .key("pk", s("control"))
        .key("sk", s("M"))
        .condition_expression(expression)
        .expression_attribute_names(alias, field)
        .expression_attribute_values(":value", s(value))
        .build()
        .unwrap()
}
#[test]
fn control_predicates_merge_on_one_exact_item_without_alias_reinterpretation() {
    let left = check("#d = :value", "#d", "domain", "scope");
    let right = check("#domain = :value", "#domain", "domain", "scope");
    let merged = merge_control_guard(left.clone(), right.clone()).unwrap();
    assert_eq!(merged.key, left.key);
    assert_eq!(
        merged.condition_expression,
        "(#d = :value) AND (#domain = :value)"
    );
    assert_eq!(
        merged.expression_attribute_values.as_ref().unwrap().len(),
        1
    );
    assert_eq!(merged.expression_attribute_names.as_ref().unwrap().len(), 2);
    let mut other = right.clone();
    other.table_name = "other".into();
    assert!(merge_control_guard(left.clone(), other).is_err());
    let mut other = right.clone();
    other.key.insert("pk".into(), s("different"));
    assert!(merge_control_guard(left.clone(), other).is_err());
    assert!(
        merge_control_guard(
            left.clone(),
            check("#d = :value", "#d", "incarnation", "scope")
        )
        .is_err()
    );
    assert!(
        merge_control_guard(
            left,
            check("#domain = :value", "#domain", "domain", "different")
        )
        .is_err()
    );
}
