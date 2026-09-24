//! Deterministic natural-language translator.
//!
//! This is not a language model and does not pretend to be one. It recognizes
//! the shapes agents actually send for the v0.1 operation set:
//! counts, sums, averages, group-bys, top-N, comparisons and relative time
//! windows. It refuses everything else with a message that says what to do
//! instead.
//!
//! Why rules and not a language model?
//!
//! * Latency. Translation runs in-process in microseconds; a hosted model adds
//!   hundreds of milliseconds to seconds to every question, on top of the
//!   calling agent's own model round trip.
//! * Determinism. The same question over the same schema yields the same plan,
//!   so the corpus in `tests/nl.rs` is an assertion about behaviour, not about
//!   a model version.
//! * Nothing leaves the process: no API key, no prompt, no data sent anywhere.
//! * A refusal here ("I could not turn that into a plan; name the column")
//!   is strictly better than a plausible-looking wrong query, and a question
//!   that needs two tables is refused rather than answered from one.
//!
//! Anything it produces still goes through the validator, so a mistaken column
//! guess becomes an error rather than a wrong answer.

use adb_core::{AdbError, ColumnSchema, DataType, Result, SemanticType, TableName, TableSchema};
use adb_planner::AggregateFunc;
use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};
use serde_json::json;

use crate::plan::{FilterOp, FilterSpec, MetricSpec, Operation, OrderSpec, PlanRequest};
use crate::translate::{IntentTranslator, Translation, TranslationContext};

/// How many groups a "which ...?" question returns when it does not say.
const DEFAULT_RANKING_LIMIT: usize = 10;

/// Words that carry no meaning for column resolution.
const STOPWORDS: [&str; 18] = [
    "the", "of", "a", "an", "all", "each", "every", "me", "my", "our", "their", "in", "for",
    "with", "and", "that", "which", "was",
];

#[derive(Debug, Default, Clone, Copy)]
pub struct RuleTranslator;

impl RuleTranslator {
    pub fn new() -> Self {
        Self
    }
}

impl IntentTranslator for RuleTranslator {
    fn name(&self) -> &'static str {
        "rules"
    }

    fn translate(&self, request: &str, context: &TranslationContext) -> Result<Translation> {
        context.require_tables()?;
        let mut parser = Parser::new(request, context)?;
        let plan = parser.parse()?;
        Ok(Translation {
            plan,
            translator: "rules",
            interpretation: parser.interpretation(),
        })
    }
}

struct Parser<'a> {
    context: &'a TranslationContext,
    schema: &'a TableSchema,
    words: Vec<String>,
    used: Vec<bool>,
    filters: Vec<FilterSpec>,
    group_by: Vec<String>,
    metrics: Vec<MetricSpec>,
    order_by: Vec<OrderSpec>,
    limit: Option<usize>,
    notes: Vec<String>,
}

impl<'a> Parser<'a> {
    fn new(request: &str, context: &'a TranslationContext) -> Result<Self> {
        let words = tokenize(request);
        if words.is_empty() {
            return Err(AdbError::bad_request("the request is empty"));
        }
        let schema = pick_table(&words, context)?;
        let used = vec![false; words.len()];
        Ok(Self {
            context,
            schema,
            words,
            used,
            filters: Vec::new(),
            group_by: Vec::new(),
            metrics: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            notes: Vec::new(),
        })
    }

    fn parse(&mut self) -> Result<PlanRequest> {
        self.reject_multi_table()?;
        self.parse_relative_time()?;
        self.parse_absolute_time()?;
        self.parse_null_checks();
        self.parse_comparisons()?;
        self.parse_equalities()?;
        self.parse_explicit_order();
        self.parse_top_n()?;
        self.parse_subject()?;
        self.parse_group_by()?;
        self.parse_metrics()?;
        self.finish()
    }

    fn finish(&mut self) -> Result<PlanRequest> {
        let aggregating = !self.metrics.is_empty() || !self.group_by.is_empty();
        if aggregating && self.metrics.is_empty() {
            // "orders by country" with no metric named: the useful answer is a
            // count per group.
            self.metrics.push(MetricSpec::count());
            self.notes.push("counting rows per group".to_string());
        }

        // A "top N" over an aggregate orders by the first metric.
        if aggregating && self.limit.is_some() && self.order_by.is_empty() {
            let alias = metric_alias(&self.metrics[0]);
            self.order_by.push(OrderSpec::desc(alias));
        }

        // "which companies ...?" is a request for a ranking, so order by the
        // metric and return a shortlist rather than every group.
        let ranking = self
            .words
            .first()
            .map(|w| matches!(w.as_str(), "which" | "what" | "who"))
            .unwrap_or(false);
        if ranking && aggregating && !self.group_by.is_empty() {
            if self.order_by.is_empty() {
                let alias = metric_alias(&self.metrics[0]);
                self.order_by.push(OrderSpec::desc(alias));
            }
            if self.limit.is_none() {
                self.limit = Some(DEFAULT_RANKING_LIMIT);
                self.notes.push(format!("top {DEFAULT_RANKING_LIMIT}"));
            }
        }

        if !aggregating
            && self.filters.is_empty()
            && self.limit.is_none()
            && self.order_by.is_empty()
            && !self.looks_like_a_listing()
        {
            return Err(AdbError::bad_request(format!(
                "could not turn {:?} into a query plan. Try naming a metric (count, total, \
                 average), a column to group by, or a condition, or send a structured plan. \
                 Columns of {}: {}",
                self.words.join(" "),
                self.schema.name,
                self.schema.default_projection().join(", ")
            )));
        }

        let operation = if aggregating {
            Operation::Aggregate
        } else {
            Operation::Select
        };
        Ok(PlanRequest {
            operation,
            table: self.schema.name.to_string(),
            columns: None,
            filters: std::mem::take(&mut self.filters),
            group_by: std::mem::take(&mut self.group_by),
            metrics: std::mem::take(&mut self.metrics),
            order_by: std::mem::take(&mut self.order_by),
            limit: self.limit,
            offset: None,
        })
    }

    fn interpretation(&self) -> String {
        if self.notes.is_empty() {
            format!("reading {}", self.schema.name)
        } else {
            format!("reading {} ({})", self.schema.name, self.notes.join("; "))
        }
    }

    /// A bare "show me the orders" is a legitimate listing request.
    fn looks_like_a_listing(&self) -> bool {
        const LISTING: [&str; 8] = [
            "show", "list", "find", "get", "select", "fetch", "display", "give",
        ];
        self.words.iter().any(|w| LISTING.contains(&w.as_str()))
    }

    /// Two tables in one request means a join, which v0.1 does not execute.
    fn reject_multi_table(&self) -> Result<()> {
        let mentioned: Vec<&str> = self
            .context
            .tables
            .iter()
            .map(|t| t.name.as_str())
            .filter(|name| self.words.iter().any(|w| matches_name(w, name)))
            .collect();
        if mentioned.len() > 1 {
            return Err(AdbError::Unsupported(format!(
                "that request spans {} tables ({}), which needs a join; v0.1 queries one table at \
                 a time",
                mentioned.len(),
                mentioned.join(" and ")
            )));
        }
        // Anti-joins ("customers who haven't ordered") are the readme's own
        // example and are explicitly out of scope, so say so plainly.
        const ANTI_JOIN: [&str; 5] = ["haven", "hasn", "didn", "never", "without"];
        if self
            .words
            .iter()
            .any(|w| ANTI_JOIN.iter().any(|marker| w.starts_with(marker)))
        {
            return Err(AdbError::Unsupported(
                "questions about rows that are missing from another table need a join or an \
                 anti-join, which v0.1 does not execute yet"
                    .to_string(),
            ));
        }
        Ok(())
    }

    // --- filters ---------------------------------------------------------

    /// "last 90 days", "past 3 months", "this year", "last month".
    fn parse_relative_time(&mut self) -> Result<()> {
        let now = self.context.now;
        for i in 0..self.words.len() {
            if self.used[i] {
                continue;
            }
            let word = self.words[i].clone();
            if !matches!(
                word.as_str(),
                "last" | "past" | "previous" | "this" | "recent"
            ) {
                continue;
            }
            // "last 90 days"
            if let (Some(count), Some(unit)) = (
                self.words.get(i + 1).and_then(|w| w.parse::<i64>().ok()),
                self.words.get(i + 2).cloned(),
            ) {
                if let Some(span) = duration_of(&unit, count) {
                    let column = self.time_column_near(i)?;
                    let from = now - span;
                    self.push_time_filter(&column, FilterOp::Gte, from);
                    self.notes
                        .push(format!("{column} in the last {count} {unit}"));
                    self.used[i] = true;
                    self.used[i + 1] = true;
                    self.used[i + 2] = true;
                    continue;
                }
            }
            // "last month", "this year"
            if let Some(unit) = self.words.get(i + 1).cloned() {
                let previous = word != "this";
                if let Some((from, to)) = calendar_window(&unit, previous, now) {
                    let column = self.time_column_near(i)?;
                    self.push_time_filter(&column, FilterOp::Gte, from);
                    self.push_time_filter(&column, FilterOp::Lt, to);
                    self.notes.push(format!("{column} within {word} {unit}"));
                    self.used[i] = true;
                    self.used[i + 1] = true;
                }
            }
        }
        Ok(())
    }

    /// "since 2026-01-01", "before 2026-03-01", "in 2026".
    fn parse_absolute_time(&mut self) -> Result<()> {
        for i in 0..self.words.len() {
            if self.used[i] {
                continue;
            }
            let op = match self.words[i].as_str() {
                "since" | "from" => FilterOp::Gte,
                "after" => FilterOp::Gt,
                "before" | "until" => FilterOp::Lt,
                _ => continue,
            };
            let Some(next) = self.words.get(i + 1).cloned() else {
                continue;
            };
            if adb_core::types::parse_timestamp_micros(&next).is_err() {
                continue;
            }
            let column = self.time_column_near(i)?;
            self.filters.push(FilterSpec::new(&column, op, json!(next)));
            self.notes.push(format!("{column} {} {next}", op.as_str()));
            self.used[i] = true;
            self.used[i + 1] = true;
        }

        // A bare year: "in 2026", "during 2026".
        for i in 0..self.words.len() {
            if self.used[i] {
                continue;
            }
            let Ok(year) = self.words[i].parse::<i32>() else {
                continue;
            };
            if !(1970..=2200).contains(&year) {
                continue;
            }
            let preceded_by_in = i
                .checked_sub(1)
                .map(|j| matches!(self.words[j].as_str(), "in" | "during" | "for"))
                .unwrap_or(false);
            if !preceded_by_in {
                continue;
            }
            let column = self.time_column_near(i)?;
            let from = Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0).single();
            let to = Utc.with_ymd_and_hms(year + 1, 1, 1, 0, 0, 0).single();
            if let (Some(from), Some(to)) = (from, to) {
                self.push_time_filter(&column, FilterOp::Gte, from);
                self.push_time_filter(&column, FilterOp::Lt, to);
                self.notes.push(format!("{column} during {year}"));
                self.used[i] = true;
            }
        }
        Ok(())
    }

    fn parse_null_checks(&mut self) {
        for i in 0..self.words.len() {
            if self.used[i] || self.words[i] != "is" {
                continue;
            }
            let (op, consumed) = match (
                self.words.get(i + 1).map(String::as_str),
                self.words.get(i + 2).map(String::as_str),
            ) {
                (Some("null"), _) | (Some("empty"), _) | (Some("missing"), _) => {
                    (FilterOp::IsNull, 2)
                }
                (Some("not"), Some("null")) | (Some("not"), Some("empty")) => {
                    (FilterOp::IsNotNull, 3)
                }
                _ => continue,
            };
            let Some((column, from)) = self.column_before(i) else {
                continue;
            };
            self.filters.push(FilterSpec::unary(&column, op));
            self.notes.push(format!("{column} {}", op.as_str()));
            for j in from..i + consumed {
                if j < self.used.len() {
                    self.used[j] = true;
                }
            }
        }
    }

    /// "amount greater than 100", "qty over 5", "at least 3".
    fn parse_comparisons(&mut self) -> Result<()> {
        let phrases: [(&[&str], FilterOp); 8] = [
            (&["greater", "than"], FilterOp::Gt),
            (&["more", "than"], FilterOp::Gt),
            (&["less", "than"], FilterOp::Lt),
            (&["fewer", "than"], FilterOp::Lt),
            (&["at", "least"], FilterOp::Gte),
            (&["at", "most"], FilterOp::Lte),
            (&["over"], FilterOp::Gt),
            (&["under"], FilterOp::Lt),
        ];
        for i in 0..self.words.len() {
            if self.used[i] {
                continue;
            }
            for (phrase, op) in phrases {
                if !self.matches_phrase(i, phrase) {
                    continue;
                }
                let value_at = i + phrase.len();
                let Some(number) = self.words.get(value_at).and_then(|w| parse_number(w)) else {
                    continue;
                };
                let Some((column, from)) = self.column_before(i) else {
                    continue;
                };
                self.filters
                    .push(FilterSpec::new(&column, op, number.clone()));
                self.notes
                    .push(format!("{column} {} {number}", op.as_str()));
                for j in from..=value_at {
                    self.used[j] = true;
                }
                break;
            }
        }
        Ok(())
    }

    /// "country is uae", "status = active", "where kind is 'click'".
    fn parse_equalities(&mut self) -> Result<()> {
        for i in 0..self.words.len() {
            if self.used[i] {
                continue;
            }
            let op = match self.words[i].as_str() {
                "is" | "equals" | "=" | "==" => FilterOp::Eq,
                _ => continue,
            };
            // "is not x"
            let (op, value_at) = match self.words.get(i + 1).map(String::as_str) {
                Some("not") => (FilterOp::Ne, i + 2),
                _ => (op, i + 1),
            };
            let Some(raw) = self.words.get(value_at).cloned() else {
                continue;
            };
            if self.used[value_at] || STOPWORDS.contains(&raw.as_str()) {
                continue;
            }
            let Some((column, from)) = self.column_before(i) else {
                continue;
            };
            let column_schema = self.schema.column(&column).cloned();
            let value = match (column_schema.map(|c| c.data_type), parse_number(&raw)) {
                (Some(DataType::Int64) | Some(DataType::Float64), Some(number)) => number,
                _ => json!(raw),
            };
            self.filters
                .push(FilterSpec::new(&column, op, value.clone()));
            self.notes.push(format!("{column} {} {value}", op.as_str()));
            for j in from..=value_at {
                self.used[j] = true;
            }
        }
        Ok(())
    }

    // --- shape -----------------------------------------------------------

    fn parse_explicit_order(&mut self) {
        for i in 0..self.words.len() {
            let is_order_phrase = matches!(self.words[i].as_str(), "sorted" | "ordered" | "order")
                && self.words.get(i + 1).map(String::as_str) == Some("by");
            if !is_order_phrase {
                continue;
            }
            let Some((column, _, end)) = self.column_after(i + 2) else {
                continue;
            };
            let descending = self
                .words
                .get(end + 1)
                .map(|w| matches!(w.as_str(), "desc" | "descending" | "highest" | "down"))
                .unwrap_or(false);
            self.order_by.push(if descending {
                OrderSpec::desc(&column)
            } else {
                OrderSpec::asc(&column)
            });
            self.notes.push(format!(
                "sorted by {column} {}",
                if descending {
                    "descending"
                } else {
                    "ascending"
                }
            ));
            for j in i..=end.min(self.words.len() - 1) {
                self.used[j] = true;
            }
            if descending && end + 1 < self.used.len() {
                self.used[end + 1] = true;
            }
        }
    }

    /// "top 20 customers by revenue", "bottom 5", "first 10", "limit 100".
    fn parse_top_n(&mut self) -> Result<()> {
        for i in 0..self.words.len() {
            if self.used[i] {
                continue;
            }
            let word = self.words[i].as_str();
            let ascending = matches!(word, "bottom" | "lowest" | "smallest" | "cheapest");
            let descending = matches!(word, "top" | "highest" | "largest" | "biggest" | "best");
            let neutral = matches!(word, "first" | "limit" | "any");
            if !(ascending || descending || neutral) {
                continue;
            }
            let Some(count) = self.words.get(i + 1).and_then(|w| w.parse::<usize>().ok()) else {
                continue;
            };
            self.limit = Some(count);
            self.used[i] = true;
            self.used[i + 1] = true;
            self.notes.push(format!("at most {count} rows"));

            // "top N <phrase> by <column>": if <phrase> names a column it is the
            // grouping; if it names the table it is just a row listing.
            if let Some(by) = self.find_word_after(i + 2, "by") {
                if let Some((sort_column, _, end)) = self.column_after(by + 1) {
                    let subject = self.phrase_between(i + 2, by);
                    let group = subject
                        .as_ref()
                        .and_then(|s| self.resolve(s))
                        .map(|column| column.name.clone());
                    if let Some(group) = group {
                        self.group_by.push(group.clone());
                        self.notes.push(format!("grouped by {group}"));
                        let aggregation =
                            default_metric_for(&self.schema.require_column(&sort_column)?.clone());
                        let alias = metric_alias(&aggregation);
                        self.metrics.push(aggregation);
                        self.order_by.push(if ascending {
                            OrderSpec::asc(&alias)
                        } else {
                            OrderSpec::desc(&alias)
                        });
                    } else {
                        self.order_by.push(if ascending {
                            OrderSpec::asc(&sort_column)
                        } else {
                            OrderSpec::desc(&sort_column)
                        });
                        self.notes.push(format!(
                            "sorted by {sort_column} {}",
                            if ascending { "ascending" } else { "descending" }
                        ));
                    }
                    for j in i + 2..=end.min(self.words.len() - 1) {
                        self.used[j] = true;
                    }
                }
            }
        }
        Ok(())
    }

    /// "which companies ...", "what sources ...": the subject of a ranking
    /// question is the grouping.
    ///
    /// Only label-like columns qualify: "what is the total revenue" must not
    /// group by revenue.
    fn parse_subject(&mut self) -> Result<()> {
        if !self.group_by.is_empty() {
            return Ok(());
        }
        let Some(first) = self.words.first().cloned() else {
            return Ok(());
        };
        if !matches!(first.as_str(), "which" | "what" | "who") {
            return Ok(());
        }
        if self
            .words
            .get(1)
            .map(|w| matches!(w.as_str(), "is" | "are" | "was" | "were"))
            .unwrap_or(false)
        {
            return Ok(());
        }
        let Some((column, start, end)) = self.column_after(1) else {
            return Ok(());
        };
        let schema = self.schema.require_column(&column)?;
        let groupable = matches!(
            schema.semantic_type,
            Some(SemanticType::Category)
                | Some(SemanticType::Country)
                | Some(SemanticType::Id)
                | Some(SemanticType::Email)
        ) || (schema.data_type == DataType::Utf8
            && schema.default_aggregation.is_none());
        if !groupable {
            return Ok(());
        }
        self.group_by.push(column.clone());
        self.notes.push(format!("grouped by {column}"));
        for j in start..=end.min(self.words.len() - 1) {
            self.used[j] = true;
        }
        Ok(())
    }

    fn parse_group_by(&mut self) -> Result<()> {
        for i in 0..self.words.len() {
            if self.used[i] || !self.group_by.is_empty() {
                continue;
            }
            let keyword = matches!(self.words[i].as_str(), "by" | "per" | "across" | "each");
            if !keyword {
                continue;
            }
            // "grouped by" / "broken down by" already handled by the same "by".
            let Some((column, start, end)) = self.column_after(i + 1) else {
                continue;
            };
            self.group_by.push(column.clone());
            self.notes.push(format!("grouped by {column}"));
            self.used[i] = true;
            for j in start..=end.min(self.words.len() - 1) {
                self.used[j] = true;
            }
        }
        Ok(())
    }

    fn parse_metrics(&mut self) -> Result<()> {
        // Count phrasings first: they take no column.
        for i in 0..self.words.len() {
            if self.used[i] {
                continue;
            }
            let counts = match self.words[i].as_str() {
                "how" => self.words.get(i + 1).map(String::as_str) == Some("many"),
                "count" | "counts" => true,
                "number" => self.words.get(i + 1).map(String::as_str) == Some("of"),
                _ => false,
            };
            if counts {
                self.metrics.push(MetricSpec::count());
                self.notes.push("counting rows".to_string());
                self.used[i] = true;
                return Ok(());
            }
        }

        for i in 0..self.words.len() {
            if self.used[i] {
                continue;
            }
            let Some(func) = aggregate_word(self.words[i].as_str()) else {
                continue;
            };
            let column = match self.column_after(i + 1) {
                Some((column, start, end)) => {
                    for j in start..=end.min(self.words.len() - 1) {
                        self.used[j] = true;
                    }
                    Some(column)
                }
                None => self.default_measure(func)?,
            };
            let Some(column) = column else { continue };
            let schema = self.schema.require_column(&column)?;
            if !metric_is_possible(func, schema) {
                return Err(AdbError::bad_request(format!(
                    "{}({}) is not meaningful for a {} column; pick another column",
                    func.as_str(),
                    column,
                    schema.data_type
                )));
            }
            self.metrics
                .push(MetricSpec::new(func, Some(&column), None));
            self.notes.push(format!("{}({column})", func.as_str()));
            self.used[i] = true;
            return Ok(());
        }

        // Last resort: a column that declares how it wants to be aggregated,
        // named directly or matched through its description. "most likely to
        // convert" finds a `score` column described as a conversion likelihood
        // and uses the aggregation the schema asked for. This runs only after
        // the explicit function words above, so "average amount" stays an
        // average even when `amount` declares `sum`.
        for i in 0..self.words.len() {
            if self.used[i] {
                continue;
            }
            let word = self.words[i].clone();
            if STOPWORDS.contains(&word.as_str()) {
                continue;
            }
            let measure = self.resolve(&word).and_then(|column| {
                column
                    .default_aggregation
                    .map(|agg| (column.name.clone(), agg))
            });
            if let Some((name, aggregation)) = measure {
                let func = AggregateFunc::from_aggregation(aggregation);
                self.metrics.push(MetricSpec::new(func, Some(&name), None));
                self.notes.push(format!("{}({name})", func.as_str()));
                self.used[i] = true;
                return Ok(());
            }
        }
        Ok(())
    }

    /// The obvious measure column when the request says "total" without naming
    /// one: a column that declares this aggregation, or the only currency column.
    fn default_measure(&self, func: AggregateFunc) -> Result<Option<String>> {
        let declared: Vec<&ColumnSchema> = self
            .schema
            .columns
            .iter()
            .filter(|c| {
                c.default_aggregation
                    .map(|a| AggregateFunc::from_aggregation(a) == func)
                    .unwrap_or(false)
            })
            .collect();
        if declared.len() == 1 {
            return Ok(Some(declared[0].name.clone()));
        }
        let currency: Vec<&ColumnSchema> = self
            .schema
            .columns
            .iter()
            .filter(|c| c.semantic_type == Some(SemanticType::Currency))
            .collect();
        if currency.len() == 1 {
            return Ok(Some(currency[0].name.clone()));
        }
        let numeric: Vec<&ColumnSchema> = self
            .schema
            .columns
            .iter()
            .filter(|c| {
                c.data_type.is_numeric()
                    && c.semantic_type
                        .as_ref()
                        .map(|s| !s.is_aggregation_hostile())
                        .unwrap_or(true)
            })
            .collect();
        if numeric.len() == 1 {
            return Ok(Some(numeric[0].name.clone()));
        }
        Err(AdbError::bad_request(format!(
            "which column should be {}med? {} has several candidates: {}",
            func.as_str(),
            self.schema.name,
            numeric
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )))
    }

    // --- helpers ---------------------------------------------------------

    fn matches_phrase(&self, at: usize, phrase: &[&str]) -> bool {
        phrase
            .iter()
            .enumerate()
            .all(|(offset, word)| self.words.get(at + offset).map(String::as_str) == Some(*word))
    }

    fn find_word_after(&self, from: usize, word: &str) -> Option<usize> {
        (from..self.words.len()).find(|i| self.words[*i] == word)
    }

    fn phrase_between(&self, from: usize, to: usize) -> Option<String> {
        if from >= to || to > self.words.len() {
            return None;
        }
        let joined = self.words[from..to]
            .iter()
            .filter(|w| !STOPWORDS.contains(&w.as_str()))
            .cloned()
            .collect::<Vec<_>>()
            .join(" ");
        if joined.is_empty() {
            None
        } else {
            Some(joined)
        }
    }

    /// Resolve a column named in the 1-3 words before `at`.
    ///
    /// Copulas and articles are skipped first, so "amount is greater than 100"
    /// finds `amount` rather than giving up on "amount is".
    fn column_before(&self, at: usize) -> Option<(String, usize)> {
        const FILLER: [&str; 11] = [
            "is", "are", "was", "were", "be", "been", "the", "a", "an", "of", "=",
        ];
        let mut end = at;
        while end > 0 && FILLER.contains(&self.words[end - 1].as_str()) {
            end -= 1;
        }
        for length in 1..=3usize {
            if end < length {
                break;
            }
            let start = end - length;
            let phrase = self.words[start..end].join(" ");
            if let Some(column) = self.resolve(&phrase) {
                return Some((column.name.clone(), start));
            }
        }
        None
    }

    /// Resolve a column named at or shortly after `at`; returns the word range.
    fn column_after(&self, at: usize) -> Option<(String, usize, usize)> {
        for skip in 0..=2usize {
            let start = at + skip;
            if start >= self.words.len() {
                break;
            }
            for length in (1..=3usize).rev() {
                let end = (start + length).min(self.words.len());
                if end <= start {
                    continue;
                }
                let phrase = self.words[start..end].join(" ");
                if let Some(column) = self.resolve(&phrase) {
                    return Some((column.name.clone(), start, end - 1));
                }
            }
        }
        None
    }

    fn resolve(&self, phrase: &str) -> Option<&ColumnSchema> {
        resolve_column(self.schema, phrase)
    }

    /// The time column a temporal filter should apply to: one named nearby, or
    /// the table's obvious timestamp.
    fn time_column_near(&self, at: usize) -> Result<String> {
        if let Some((column, _)) = self.column_before(at) {
            let schema = self.schema.require_column(&column)?;
            if matches!(schema.data_type, DataType::Timestamp | DataType::Date) {
                return Ok(column);
            }
        }
        primary_time_column(self.schema)
            .map(|c| c.name.clone())
            .ok_or_else(|| {
                AdbError::bad_request(format!(
                    "{} has no timestamp column, so a time window cannot be applied",
                    self.schema.name
                ))
            })
    }

    fn push_time_filter(&mut self, column: &str, op: FilterOp, at: DateTime<Utc>) {
        self.filters.push(FilterSpec::new(
            column,
            op,
            json!(at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)),
        ));
    }
}

fn metric_alias(metric: &MetricSpec) -> String {
    metric.alias.clone().unwrap_or_else(|| {
        adb_planner::AggregateExpr::default_alias(metric.function, metric.column.as_deref())
    })
}

fn default_metric_for(column: &ColumnSchema) -> MetricSpec {
    let func = column
        .default_aggregation
        .map(AggregateFunc::from_aggregation)
        .unwrap_or(if column.data_type.is_additive() {
            AggregateFunc::Sum
        } else {
            AggregateFunc::Count
        });
    match func {
        AggregateFunc::Count => MetricSpec::count(),
        other => MetricSpec::new(other, Some(&column.name), None),
    }
}

fn metric_is_possible(func: AggregateFunc, column: &ColumnSchema) -> bool {
    match func {
        AggregateFunc::Count => true,
        AggregateFunc::Sum | AggregateFunc::Avg => {
            column.data_type.is_additive()
                && !column
                    .semantic_type
                    .as_ref()
                    .map(SemanticType::is_aggregation_hostile)
                    .unwrap_or(false)
        }
        AggregateFunc::Min | AggregateFunc::Max => column.data_type.is_ordered(),
    }
}

fn aggregate_word(word: &str) -> Option<AggregateFunc> {
    Some(match word {
        "total" | "sum" | "summed" | "revenue" | "spend" => AggregateFunc::Sum,
        "average" | "avg" | "mean" => AggregateFunc::Avg,
        "minimum" | "min" | "earliest" => AggregateFunc::Min,
        "maximum" | "max" | "latest" => AggregateFunc::Max,
        _ => return None,
    })
}

fn duration_of(unit: &str, count: i64) -> Option<Duration> {
    let days = match unit.trim_end_matches('s') {
        "day" => 1,
        "week" => 7,
        "month" => 30,
        "quarter" => 91,
        "year" => 365,
        "hour" => return Some(Duration::hours(count)),
        "minute" => return Some(Duration::minutes(count)),
        _ => return None,
    };
    Some(Duration::days(days * count))
}

/// Calendar-aligned windows: "this month", "last year".
fn calendar_window(
    unit: &str,
    previous: bool,
    now: DateTime<Utc>,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let start_of_day = Utc
        .with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
        .single()?;
    match unit.trim_end_matches('s') {
        "day" | "today" => {
            let start = if previous {
                start_of_day - Duration::days(1)
            } else {
                start_of_day
            };
            Some((start, start + Duration::days(1)))
        }
        "week" => {
            let weekday = now.weekday().num_days_from_monday() as i64;
            let start = start_of_day - Duration::days(weekday);
            let start = if previous {
                start - Duration::days(7)
            } else {
                start
            };
            Some((start, start + Duration::days(7)))
        }
        "month" => {
            let (year, month) = if previous {
                if now.month() == 1 {
                    (now.year() - 1, 12)
                } else {
                    (now.year(), now.month() - 1)
                }
            } else {
                (now.year(), now.month())
            };
            let start = Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0).single()?;
            let (next_year, next_month) = if month == 12 {
                (year + 1, 1)
            } else {
                (year, month + 1)
            };
            let end = Utc
                .with_ymd_and_hms(next_year, next_month, 1, 0, 0, 0)
                .single()?;
            Some((start, end))
        }
        "quarter" => {
            let quarter = (now.month() - 1) / 3;
            let (year, quarter) = if previous {
                if quarter == 0 {
                    (now.year() - 1, 3)
                } else {
                    (now.year(), quarter - 1)
                }
            } else {
                (now.year(), quarter)
            };
            let start = Utc
                .with_ymd_and_hms(year, quarter * 3 + 1, 1, 0, 0, 0)
                .single()?;
            let (end_year, end_month) = if quarter == 3 {
                (year + 1, 1)
            } else {
                (year, quarter * 3 + 4)
            };
            let end = Utc
                .with_ymd_and_hms(end_year, end_month, 1, 0, 0, 0)
                .single()?;
            Some((start, end))
        }
        "year" => {
            let year = if previous { now.year() - 1 } else { now.year() };
            Some((
                Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0).single()?,
                Utc.with_ymd_and_hms(year + 1, 1, 1, 0, 0, 0).single()?,
            ))
        }
        _ => None,
    }
}

fn parse_number(word: &str) -> Option<serde_json::Value> {
    let cleaned: String = word.chars().filter(|c| *c != ',' && *c != '$').collect();
    if let Ok(int) = cleaned.parse::<i64>() {
        return Some(json!(int));
    }
    cleaned.parse::<f64>().ok().map(|f| json!(f))
}

fn tokenize(request: &str) -> Vec<String> {
    let lowered = request.to_lowercase();
    lowered
        // Commas are not separators: "1,000" must survive as one token. They
        // are trimmed at word edges instead.
        .split(|c: char| c.is_whitespace() || matches!(c, '(' | ')' | '?' | '!' | ';' | ':'))
        .map(|word| {
            word.trim_matches(|c: char| matches!(c, '"' | '\'' | '.' | '`' | ','))
                .to_string()
        })
        .filter(|word| !word.is_empty())
        .collect()
}

fn matches_name(word: &str, name: &str) -> bool {
    let normalized = word.replace(['-', ' '], "_");
    normalized == name
        || normalized.trim_end_matches('s') == name.trim_end_matches('s')
        || normalized.replace('_', "") == name.replace('_', "")
}

fn pick_table<'a>(words: &[String], context: &'a TranslationContext) -> Result<&'a TableSchema> {
    if let Some(table) = context
        .tables
        .iter()
        .find(|t| words.iter().any(|w| matches_name(w, t.name.as_str())))
    {
        return Ok(table);
    }
    if let Some(default) = &context.default_table {
        if let Some(table) = context.table(default.as_str()) {
            return Ok(table);
        }
    }
    if context.tables.len() == 1 {
        return Ok(&context.tables[0]);
    }
    Err(AdbError::bad_request(format!(
        "which table? this database has: {}",
        context
            .tables
            .iter()
            .map(|t| t.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

/// Match a phrase to a column: exact name, then loose forms, then semantics.
fn resolve_column<'a>(schema: &'a TableSchema, phrase: &str) -> Option<&'a ColumnSchema> {
    let phrase = phrase.trim();
    if phrase.is_empty() {
        return None;
    }
    let normalized = phrase.replace([' ', '-'], "_");
    let squashed = normalized.replace('_', "");

    if let Some(column) = schema.column(&normalized) {
        return Some(column);
    }
    if let Some(column) = schema.columns.iter().find(|c| {
        c.name.replace('_', "") == squashed
            || singular(&c.name) == singular(&normalized)
            || singular(&c.name.replace('_', "")) == singular(&squashed)
    }) {
        return Some(column);
    }

    // Semantic synonyms, used only when they are unambiguous.
    let semantic = match phrase {
        "revenue" | "sales" | "amount" | "spend" | "value" | "money" | "price" | "cost" => {
            Some(SemanticType::Currency)
        }
        "country" | "market" | "region" | "nation" => Some(SemanticType::Country),
        "date" | "time" | "when" | "day" | "timestamp" => Some(SemanticType::Timestamp),
        "category" | "kind" | "type" | "segment" | "status" => Some(SemanticType::Category),
        "email" | "e-mail" => Some(SemanticType::Email),
        _ => None,
    };
    if let Some(semantic) = semantic {
        let matches: Vec<&ColumnSchema> = schema
            .columns
            .iter()
            .filter(|c| c.semantic_type.as_ref() == Some(&semantic))
            .collect();
        if matches.len() == 1 {
            return Some(matches[0]);
        }
        if semantic == SemanticType::Timestamp {
            return primary_time_column(schema);
        }
    }

    // A unique column whose name starts with the phrase: "created" -> created_at.
    if normalized.len() >= 4 {
        let prefixed: Vec<&ColumnSchema> = schema
            .columns
            .iter()
            // Only a phrase that is a prefix of the column name, never the
            // reverse: "lifetime_value desc" must not resolve to lifetime_value
            // and swallow the sort direction.
            .filter(|c| c.name.starts_with(&normalized))
            .collect();
        if prefixed.len() == 1 {
            return Some(prefixed[0]);
        }
    }

    // Last resort: a unique description match.
    let described: Vec<&ColumnSchema> = schema
        .columns
        .iter()
        .filter(|c| {
            c.description
                .as_ref()
                .map(|d| d.to_lowercase().contains(phrase))
                .unwrap_or(false)
        })
        .collect();
    if described.len() == 1 {
        return Some(described[0]);
    }
    None
}

/// Crude English singularization, enough for column names: "countries" ->
/// "country", "orders" -> "order".
fn singular(word: &str) -> String {
    if let Some(stem) = word.strip_suffix("ies") {
        return format!("{stem}y");
    }
    for suffix in ["ses", "xes", "zes", "ches", "shes"] {
        if let Some(stem) = word.strip_suffix(suffix) {
            return format!("{stem}{}", &suffix[..suffix.len() - 2]);
        }
    }
    word.strip_suffix('s').unwrap_or(word).to_string()
}

/// The column a time window applies to by default.
fn primary_time_column(schema: &TableSchema) -> Option<&ColumnSchema> {
    let temporal: Vec<&ColumnSchema> = schema
        .columns
        .iter()
        .filter(|c| matches!(c.data_type, DataType::Timestamp | DataType::Date))
        .collect();
    if temporal.len() == 1 {
        return Some(temporal[0]);
    }
    temporal
        .iter()
        .find(|c| c.semantic_type == Some(SemanticType::Timestamp))
        .or_else(|| temporal.iter().find(|c| c.name.contains("created")))
        .copied()
}

/// Table name resolution, exposed for the MCP layer's error messages.
pub fn table_for_request(request: &str, context: &TranslationContext) -> Result<TableName> {
    let words = tokenize(request);
    pick_table(&words, context).map(|t| t.name.clone())
}
