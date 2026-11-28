//! The declarative-jobs examples in the usage guide are loadable declarations: the fenced
//! jobs block in docs/advanced.md parses into the real config types, resolves into one
//! job-targeted trigger plus one alert, and the self-dev block resolves into a plan that ends at
//! deliver — publish is not a job action (ADR-084). A docs snippet that no longer loads is a doc
//! bug, so this is the drift gate for it (README policy: YAML snippets stay parseable).

use std::collections::BTreeMap;

use kian_cli::config::{ChannelConfig, CronTarget};
use kian_cli::job_config::JobsSection;

fn repo_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
}

/// The first yaml fence under the named section of the usage guide.
fn yaml_example(section: &str) -> String {
    let guide = std::fs::read_to_string(repo_root().join("docs/advanced.md")).expect("the guide");
    let start = guide
        .find(section)
        .unwrap_or_else(|| panic!("the guide carries a {section} section"));
    let rest = &guide[start..];
    let open = rest.find("```yaml").expect("a yaml fence") + "```yaml\n".len();
    let tail = &rest[open..];
    let close = tail.find("```").expect("a closing fence");
    tail[..close].to_string()
}

fn channels(names: &[&str]) -> BTreeMap<String, ChannelConfig> {
    let mut map = BTreeMap::new();
    for name in names {
        let cfg: ChannelConfig = serde_yaml::from_str(
            "type: timer
timer:
  interval_ms: 60000
",
        )
        .expect("the test channel parses");
        map.insert((*name).to_string(), cfg);
    }
    map
}

#[test]
fn the_usage_guide_job_example_is_a_loadable_declaration() {
    let yaml = yaml_example("## Declarative jobs");
    let value: serde_yaml::Value =
        serde_yaml::from_str(&yaml).expect("the guide's job block is valid yaml");
    let jobs: JobsSection = serde_yaml::from_value(value["superagent"]["jobs"].clone())
        .expect("the guide's job block matches the config schema");

    let resolved = jobs
        .resolve(
            &channels(&["imessage", "lark"]),
            &Default::default(),
            chrono::Utc::now(),
        )
        .expect("the guide's job block resolves");

    assert_eq!(
        resolved.schedules.len(),
        1,
        "one trigger lowers to one schedule"
    );
    let schedule = &resolved.schedules[0];
    assert_eq!(schedule.id, "digest-morning");
    assert!(
        matches!(&schedule.target, CronTarget::Job { job } if job == "digest"),
        "the trigger targets the job: {:?}",
        schedule.target
    );

    let job = resolved.catalog.job("digest").expect("digest is declared");
    assert_eq!(job.plan.steps.len(), 3, "gather, compose, send");
    assert_eq!(
        job.plan.steps[2].action,
        kian_superagent::job::plan::StepAction::Deliver
    );
    assert_eq!(job.triggers, vec!["digest-morning".to_string()]);

    let alerts = resolved.catalog.alerts_for("digest");
    assert_eq!(alerts.len(), 1, "one alert is declared for the job");
    assert_eq!(alerts[0].id, "digest-failed");
    assert_eq!(alerts[0].channel, "lark");
}

#[test]
fn the_self_dev_example_is_a_loadable_declaration_without_the_retired_publish_action() {
    let yaml = yaml_example("## Self-dev proposals");
    let value: serde_yaml::Value =
        serde_yaml::from_str(&yaml).expect("the guide's self-dev block is valid yaml");
    let jobs: JobsSection = serde_yaml::from_value(value["superagent"]["jobs"].clone())
        .expect("the guide's self-dev block matches the config schema");

    let resolved = jobs
        .resolve(
            &channels(&["imessage", "lark"]),
            &Default::default(),
            chrono::Utc::now(),
        )
        .expect("the guide's self-dev block resolves");

    assert!(
        resolved.schedules.is_empty(),
        "a self-dev run is manual-only: no trigger is declared"
    );
    let job = resolved
        .catalog
        .job("self-dev")
        .expect("self-dev is declared");
    assert_eq!(job.plan.steps.len(), 2, "patch, deliver");
    assert_eq!(
        job.plan.steps[0].action,
        kian_superagent::job::plan::StepAction::Agent,
        "the patch step is an agent step"
    );
    assert_eq!(
        job.plan.steps[1].action,
        kian_superagent::job::plan::StepAction::Deliver,
        "the summary step is a deliver step"
    );
    for step in &job.plan.steps {
        assert!(
            !matches!(step.action, kian_superagent::job::plan::StepAction::Publish),
            "a new plan cannot declare the retired publish action"
        );
        assert!(
            step.publish.is_none(),
            "a new plan cannot carry the retired publish field"
        );
    }
    assert!(
        !yaml.contains("action: publish"),
        "the loadable example must not declare the retired action"
    );
}
