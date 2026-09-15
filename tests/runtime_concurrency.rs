use kakune_core::{Store, WorkflowDocument, run_workflow};

fn source(limit: u32) -> String {
    format!(
        r#"apiVersion: kakune/v1
kind: Workflow
metadata: {{id: parallel, name: Parallel}}
triggers: [{{id: manual, type: kakune.trigger.manual@1}}]
entry: start
policy: {{maxParallelNodes: {limit}}}
nodes:
  - id: start
    type: kakune.flow.pass@1
    on: {{success: [left, right]}}
  - id: left
    type: kakune.flow.delay@1
    inputs: {{durationMs: {{literal: 120}}}}
    on: {{success: finish}}
  - id: right
    type: kakune.flow.delay@1
    inputs: {{durationMs: {{literal: 120}}}}
    on: {{success: finish}}
  - id: finish
    type: kakune.flow.join@1
"#
    )
}

#[test]
fn fan_out_overlaps_but_respects_the_configured_limit() {
    for limit in [1, 2] {
        let dir = std::env::temp_dir().join(format!("kakune-parallel-{}", uuid::Uuid::new_v4()));
        let store = Store::open(dir.clone()).unwrap();
        let source = source(limit);
        let workflow = WorkflowDocument::parse(&source).unwrap();
        store
            .upsert_workflow(&workflow, &source, "enabled")
            .unwrap();
        let run = run_workflow(&store, &workflow).unwrap();
        let trace = store.execution_trace(&run.id).unwrap().unwrap();
        let left = trace
            .spans
            .iter()
            .find(|span| span.node_id.as_deref() == Some("left"))
            .unwrap();
        let right = trace
            .spans
            .iter()
            .find(|span| span.node_id.as_deref() == Some("right"))
            .unwrap();
        let end = trace
            .spans
            .iter()
            .find(|span| span.node_id.as_deref() == Some("finish"))
            .unwrap();
        let overlap = left.started_at < *right.completed_at.as_ref().unwrap()
            && right.started_at < *left.completed_at.as_ref().unwrap();
        assert_eq!(
            overlap,
            limit == 2,
            "trace must prove actual overlap, not merely fan-out syntax"
        );
        assert!(end.started_at >= *left.completed_at.as_ref().unwrap());
        assert!(end.started_at >= *right.completed_at.as_ref().unwrap());
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
