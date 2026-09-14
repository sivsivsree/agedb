//! The natural-language corpus.
//!
//! Every case goes the whole way: request -> `PlanRequest` -> Query IR ->
//! validator. That is the contract that matters, because a plan that looks right
//! but fails validation is not an answer. The refusal cases are as important as
//! the successes: a wrong query that runs is worse than an error message.

use std::sync::Arc;

use adb_core::{
    Aggregation, ColumnRef, ColumnSchema, DataType, QueryLimits, SemanticType, TableName,
    TableSchema,
};
use adb_planner::{validate, Query, ValidatedQuery};
use adb_query::{IntentTranslator, RuleTranslator, TranslationContext};
use chrono::{TimeZone, Utc};

fn orders() -> Arc<TableSchema> {
    let mut schema = TableSchema::new(
        TableName::new("orders").unwrap(),
        vec![
            ColumnSchema::new("id", DataType::Int64)
                .required()
                .semantic(SemanticType::Id),
            ColumnSchema {
                references: Some(ColumnRef {
                    table: "customers".into(),
                    column: "id".into(),
                }),
                ..ColumnSchema::new("customer_id", DataType::Int64).semantic(SemanticType::Id)
            },
            ColumnSchema::new("country", DataType::Utf8).semantic(SemanticType::Country),
            ColumnSchema::new("amount", DataType::Float64)
                .semantic(SemanticType::Currency)
                .aggregated_by(Aggregation::Sum)
                .described("Total order value before refunds."),
            ColumnSchema::new("qty", DataType::Int64).semantic(SemanticType::Quantity),
            ColumnSchema::new("created_at", DataType::Timestamp).semantic(SemanticType::Timestamp),
            ColumnSchema::new("buyer_email", DataType::Utf8)
                .semantic(SemanticType::Email)
                .sensitive(),
        ],
    );
    schema.columns[3].currency = Some("USD".to_string());
    Arc::new(schema)
}

fn customers() -> Arc<TableSchema> {
    Arc::new(TableSchema::new(
        TableName::new("customers").unwrap(),
        vec![
            ColumnSchema::new("id", DataType::Int64)
                .required()
                .semantic(SemanticType::Id),
            ColumnSchema::new("name", DataType::Utf8),
            ColumnSchema::new("country", DataType::Utf8).semantic(SemanticType::Country),
            ColumnSchema::new("lifetime_value", DataType::Float64)
                .semantic(SemanticType::Currency)
                .aggregated_by(Aggregation::Sum),
            ColumnSchema::new("signup_date", DataType::Timestamp).semantic(SemanticType::Timestamp),
        ],
    ))
}

/// A fixed "now" so relative time windows are reproducible.
fn context() -> TranslationContext {
    TranslationContext::new(vec![orders(), customers()])
        .at(Utc.with_ymd_and_hms(2026, 9, 9, 12, 0, 0).unwrap())
}

fn translate(request: &str) -> adb_core::Result<(ValidatedQuery, String)> {
    let context = context();
    let translation = RuleTranslator::new().translate(request, &context)?;
    let schema = context
        .table(&translation.plan.table)
        .expect("translator picked a table in scope")
        .clone();
    let ir = translation.plan.to_ir()?;
    let validated = validate(&ir, &schema, &QueryLimits::unlimited())?;
    Ok((validated, translation.interpretation))
}

/// The plan tree, for readable assertions.
fn plan_of(request: &str) -> String {
    let (validated, _) = translate(request).unwrap_or_else(|e| panic!("{request:?}: {e}"));
    validated.query.explain()
}

fn assert_plan(request: &str, expected: &[&str]) {
    let plan = plan_of(request);
    for fragment in expected {
        assert!(
            plan.contains(fragment),
            "{request:?}\nexpected to contain {fragment:?}, got:\n{plan}"
        );
    }
}

#[test]
fn counting_questions() {
    assert_plan(
        "how many orders are there",
        &["aggregate", "count(*) as count", "scan orders"],
    );
    assert_plan("count the orders", &["count(*) as count"]);
    assert_plan(
        "what is the number of customers",
        &["count(*) as count", "scan customers"],
    );
}

#[test]
fn grouped_counts() {
    assert_plan(
        "count orders by country",
        &["aggregate group_by=[country]", "count(*) as count"],
    );
    assert_plan("orders per country", &["group_by=[country]", "count(*)"]);
}

#[test]
fn sums_and_averages_use_the_named_column() {
    assert_plan(
        "total amount by country in orders",
        &["group_by=[country]", "sum(amount)"],
    );
    assert_plan("average amount for orders", &["avg(amount)"]);
    assert_plan("maximum amount in orders", &["max(amount)"]);
    assert_plan("minimum qty in orders", &["min(qty)"]);
}

#[test]
fn a_measure_can_be_left_implicit_when_the_schema_declares_one() {
    // `amount` declares `aggregate with sum` and is the only currency column,
    // so "total revenue" is unambiguous even though no column is named.
    assert_plan(
        "total revenue by country in orders",
        &["group_by=[country]", "sum(amount)"],
    );
}

#[test]
fn top_n_over_groups_becomes_group_sort_limit() {
    assert_plan(
        "top 5 countries by amount in orders",
        &[
            "limit 5",
            "sort sum_amount desc",
            "aggregate group_by=[country]",
            "sum(amount)",
        ],
    );
}

#[test]
fn top_n_over_rows_sorts_without_grouping() {
    let plan = plan_of("top 10 orders by amount");
    assert!(plan.contains("limit 10"), "{plan}");
    assert!(plan.contains("sort amount desc"), "{plan}");
    assert!(!plan.contains("aggregate"), "{plan}");
}

#[test]
fn explicit_ordering_is_honoured() {
    assert_plan(
        "customers sorted by lifetime_value desc",
        &["sort lifetime_value desc"],
    );
    assert_plan("orders sorted by amount", &["sort amount asc"]);
}

#[test]
fn comparisons_become_filters() {
    assert_plan(
        "orders where amount is greater than 100",
        &["filter (amount > 100)"],
    );
    assert_plan("orders with qty at least 3", &["filter (qty >= 3)"]);
    assert_plan(
        "orders where amount is less than 50",
        &["filter (amount < 50)"],
    );
    assert_plan("orders with amount over 1,000", &["filter (amount > 1000)"]);
}

#[test]
fn equality_and_null_filters() {
    assert_plan(
        "orders where country is uae",
        &["filter (country = \"uae\")"],
    );
    assert_plan("orders where country is null", &["filter country is null"]);
    assert_plan(
        "orders where country is not null",
        &["filter country is not null"],
    );
}

#[test]
fn relative_time_windows_resolve_against_the_injected_clock() {
    // now = 2026-09-09T12:00:00Z, so 90 days back is 2026-06-11T12:00:00Z.
    assert_plan(
        "orders created in the last 90 days",
        &["created_at >= \"2026-06-11T12:00:00.000000Z\""],
    );
    assert_plan(
        "orders in the past 2 weeks",
        &["created_at >= \"2026-08-26T12:00:00.000000Z\""],
    );
    // Calendar windows are aligned, not rolling.
    assert_plan(
        "how many customers signed up last month",
        &[
            "signup_date >= \"2026-08-01T00:00:00.000000Z\"",
            "signup_date < \"2026-09-01T00:00:00.000000Z\"",
        ],
    );
    assert_plan(
        "orders this year",
        &[
            "created_at >= \"2026-01-01T00:00:00.000000Z\"",
            "created_at < \"2027-01-01T00:00:00.000000Z\"",
        ],
    );
}

#[test]
fn absolute_dates_and_years() {
    assert_plan(
        "show me orders since 2026-01-01",
        &["created_at >= \"2026-01-01T00:00:00.000000Z\""],
    );
    assert_plan(
        "orders before 2026-03-01",
        &["created_at < \"2026-03-01T00:00:00.000000Z\""],
    );
    assert_plan(
        "orders in 2026",
        &[
            "created_at >= \"2026-01-01T00:00:00.000000Z\"",
            "created_at < \"2027-01-01T00:00:00.000000Z\"",
        ],
    );
}

#[test]
fn a_bare_listing_is_a_select_of_every_non_sensitive_column() {
    let (validated, _) = translate("list orders").unwrap();
    assert_eq!(
        validated.schema.names(),
        vec![
            "id",
            "customer_id",
            "country",
            "amount",
            "qty",
            "created_at"
        ]
    );
    assert!(
        !validated
            .schema
            .names()
            .contains(&"buyer_email".to_string()),
        "a sensitive column must not appear in a `*` projection"
    );
}

#[test]
fn combined_requests_produce_a_whole_plan() {
    assert_plan(
        "top 3 countries by amount in orders in the last 30 days",
        &[
            "limit 3",
            "sort sum_amount desc",
            "aggregate group_by=[country]",
            "sum(amount)",
            "created_at >= \"2026-08-10T12:00:00.000000Z\"",
        ],
    );
}

#[test]
fn the_interpretation_explains_what_was_understood() {
    let (_, interpretation) = translate("total amount by country in orders").unwrap();
    assert!(interpretation.contains("orders"), "{interpretation}");
    assert!(
        interpretation.contains("grouped by country"),
        "{interpretation}"
    );
    assert!(interpretation.contains("sum(amount)"), "{interpretation}");
}

/// The readme's section 20 question, answered from schema metadata alone.
///
/// Nothing in the request names a column: "companies" resolves to the `company`
/// category column, and "convert" matches the description of `score`, which
/// declares `avg` as its aggregation. The ranking intent of "which ...?" adds
/// the ordering and a shortlist limit.
#[test]
fn a_ranking_question_is_answered_from_semantic_metadata() {
    let leads = Arc::new(
        TableSchema::new(
            TableName::new("leads").unwrap(),
            vec![
                ColumnSchema::new("id", DataType::Int64)
                    .required()
                    .semantic(SemanticType::Id),
                ColumnSchema::new("company", DataType::Utf8).semantic(SemanticType::Category),
                ColumnSchema::new("score", DataType::Float64)
                    .semantic(SemanticType::Score)
                    .aggregated_by(Aggregation::Avg)
                    .described("Model-predicted likelihood to convert, 0 to 1"),
                ColumnSchema::new("value", DataType::Float64)
                    .semantic(SemanticType::Currency)
                    .aggregated_by(Aggregation::Sum)
                    .described("Expected deal value"),
            ],
        )
        .with_primary_key(vec!["id".to_string()]),
    );
    let context = TranslationContext::new(vec![leads.clone()]);
    let translation = RuleTranslator::new()
        .translate("which companies look most likely to convert", &context)
        .unwrap();
    let validated = validate(
        &translation.plan.to_ir().unwrap(),
        &leads,
        &QueryLimits::unlimited(),
    )
    .unwrap();
    let plan = validated.query.explain();
    assert!(plan.contains("group_by=[company]"), "{plan}");
    assert!(plan.contains("avg(score)"), "{plan}");
    assert!(plan.contains("sort avg_score desc"), "{plan}");
    assert!(plan.contains("limit 10"), "{plan}");

    // The same shape works for a value question.
    let translation = RuleTranslator::new()
        .translate("which companies have the highest value", &context)
        .unwrap();
    let validated = validate(
        &translation.plan.to_ir().unwrap(),
        &leads,
        &QueryLimits::unlimited(),
    )
    .unwrap();
    let plan = validated.query.explain();
    assert!(plan.contains("group_by=[company]"), "{plan}");
    assert!(plan.contains("sum(value)"), "{plan}");
}

#[test]
fn requests_that_need_a_join_are_refused_clearly() {
    let err = translate("count orders joined with customers").unwrap_err();
    assert_eq!(err.code(), "unsupported");
    assert!(err.to_string().contains("join"), "{err}");

    // The readme's own example, which needs an anti-join.
    let err = translate(
        "find all customers who haven't purchased anything in the last 90 days grouped by country",
    )
    .unwrap_err();
    assert_eq!(err.code(), "unsupported");
    assert!(err.to_string().contains("anti-join"), "{err}");
}

#[test]
fn nonsense_is_refused_with_the_available_columns() {
    let err = translate("wibble the flimflam orders").unwrap_err();
    assert_eq!(err.code(), "bad_request");
    assert!(err.to_string().contains("could not turn"), "{err}");
    assert!(
        err.to_string().contains("amount"),
        "the error should list real columns: {err}"
    );
}

#[test]
fn an_ambiguous_table_asks_which_one() {
    let err = translate("how many are there").unwrap_err();
    assert!(err.to_string().contains("which table"), "{err}");
    assert!(err.to_string().contains("orders"), "{err}");
}

#[test]
fn a_default_table_removes_the_ambiguity() {
    let context = context().with_default_table(TableName::new("orders").unwrap());
    let translation = RuleTranslator::new()
        .translate("how many are there", &context)
        .unwrap();
    assert_eq!(translation.plan.table, "orders");
}

#[test]
fn meaningless_aggregations_are_refused_by_the_translator_or_the_validator() {
    // `customer_id` is a semantic id: summing it is nonsense, and it must not
    // silently become a plan.
    let err = translate("total customer_id in orders").unwrap_err();
    assert_eq!(err.code(), "bad_request");
    assert!(
        err.to_string().contains("not meaningful") || err.to_string().contains("not a measure"),
        "{err}"
    );
}

#[test]
fn an_empty_database_says_so() {
    let context = TranslationContext::new(vec![]);
    let err = RuleTranslator::new()
        .translate("how many orders", &context)
        .unwrap_err();
    assert!(err.to_string().contains("no tables yet"), "{err}");
}

#[test]
fn every_produced_plan_survives_validation() {
    // A regression net: anything the translator emits must be executable.
    let corpus = [
        "how many orders are there",
        "count orders by country",
        "total amount by country in orders",
        "average amount for orders",
        "top 5 countries by amount in orders",
        "top 10 orders by amount",
        "orders where amount is greater than 100",
        "orders where country is uae",
        "orders created in the last 90 days",
        "orders in 2026",
        "list orders",
        "customers sorted by lifetime_value desc",
        "how many customers signed up last month",
        "maximum amount in orders",
    ];
    for request in corpus {
        let (validated, _) = translate(request).unwrap_or_else(|e| panic!("{request:?}: {e}"));
        assert!(
            !validated.schema.columns.is_empty(),
            "{request:?} produced an empty output schema"
        );
        // And the plan must lower to something the executor can run.
        adb_planner::physical::build(&validated)
            .unwrap_or_else(|e| panic!("{request:?} did not lower: {e}"));
        assert!(matches!(
            validated.query,
            Query::Scan { .. }
                | Query::Filter { .. }
                | Query::Aggregate { .. }
                | Query::Sort { .. }
                | Query::Limit { .. }
                | Query::Project { .. }
        ));
    }
}
