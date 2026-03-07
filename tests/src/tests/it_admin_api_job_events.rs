//! Integration tests for the Admin API job events endpoint.

use std::time::Duration;

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use crate::testlib::{ctx::TestCtxBuilder, helpers::wait_for_job_completion};

#[tokio::test]
async fn get_job_events_with_deployed_dataset_succeeds() {
    //* Given
    let ctx = EventsTestCtx::setup_with_anvil("get_job_events_succeeds").await;

    // Mine blocks so syncing takes time
    ctx.anvil().mine(10).await.expect("failed to mine blocks");

    // Deploy with end_block so the job completes
    let job_id = ctx
        .deploy_dataset("_", "anvil_rpc", "0.0.0", Some(10))
        .await;

    //* When
    let resp = ctx.get_job_events(job_id).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "events retrieval should succeed for valid job"
    );

    let events_response: JobEventsResponse = resp
        .json()
        .await
        .expect("failed to parse events response JSON");

    // Verify response structure
    assert_eq!(events_response.job_id, job_id);
    // A deployed job should have at least a SCHEDULED event
    assert!(
        !events_response.events.is_empty(),
        "should have at least one event"
    );

    // Verify the first event is SCHEDULED
    assert_eq!(
        events_response.events[0].event_type, "SCHEDULED",
        "first event should be SCHEDULED"
    );
}

#[tokio::test]
async fn get_job_events_with_nonexistent_job_id_returns_not_found() {
    //* Given
    let ctx = EventsTestCtx::setup("get_job_events_invalid_id", Vec::<&str>::new()).await;

    //* When
    // Use a job ID that doesn't exist
    let resp = ctx.get_job_events(999999).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "events retrieval should fail for non-existent job"
    );

    let error: ErrorResponse = resp.json().await.expect("failed to parse error response");

    assert_eq!(error.error_code, "JOB_NOT_FOUND");
}

#[tokio::test]
async fn get_job_events_with_negative_job_id_returns_bad_request() {
    //* Given
    let ctx = EventsTestCtx::setup("get_job_events_negative_id", Vec::<&str>::new()).await;

    //* When
    // Use -1 as job ID which is not a valid JobId
    let resp = ctx.get_job_events(-1).await;

    //* Then
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "events retrieval should fail for invalid job ID format"
    );

    let error: ErrorResponse = resp.json().await.expect("failed to parse error response");

    assert_eq!(error.error_code, "INVALID_JOB_ID");
}

#[tokio::test]
async fn get_job_events_shows_lifecycle_transitions() {
    //* Given
    let ctx = EventsTestCtx::setup_with_anvil("get_job_events_lifecycle").await;

    // Mine blocks
    ctx.anvil().mine(3).await.expect("failed to mine blocks");

    // Deploy with end_block so the job completes
    let job_id = ctx.deploy_dataset("_", "anvil_rpc", "0.0.0", Some(3)).await;

    // Wait until the job reaches a terminal state
    let timeout = tokio::time::Duration::from_secs(30);

    wait_for_job_completion(
        &ctx.ctx.new_ampctl(),
        job_id
            .try_into()
            .expect("failed to convert job id to JobId"),
        false,
        timeout,
        Duration::from_millis(100),
    )
    .await
    .expect("failed to wait for job completion");

    //* When
    let resp = ctx.get_job_events(job_id).await;

    //* Then
    let final_events: JobEventsResponse = resp.json().await.expect("failed to parse JSON");

    // Verify we have multiple events showing the lifecycle
    assert!(
        final_events.events.len() >= 2,
        "should have at least SCHEDULED and a terminal event, got {}",
        final_events.events.len()
    );

    // Verify events are ordered by id ascending
    for window in final_events.events.windows(2) {
        assert!(
            window[0].id < window[1].id,
            "events should be ordered by id ascending"
        );
    }

    // Verify first event is SCHEDULED
    assert_eq!(final_events.events[0].event_type, "SCHEDULED");

    // Verify each event has a non-empty node_id and created_at
    for event in &final_events.events {
        assert!(!event.node_id.is_empty(), "node_id should not be empty");
        assert!(
            !event.created_at.is_empty(),
            "created_at should not be empty"
        );
    }
}

struct EventsTestCtx {
    ctx: crate::testlib::ctx::TestCtx,
}

impl EventsTestCtx {
    async fn setup(
        test_name: &str,
        manifests: impl IntoIterator<Item = impl Into<crate::testlib::ctx::ManifestRegistration>>,
    ) -> Self {
        let ctx = TestCtxBuilder::new(test_name)
            .with_dataset_manifests(manifests)
            .build()
            .await
            .expect("failed to build test context");
        Self { ctx }
    }

    async fn setup_with_anvil(test_name: &str) -> Self {
        let ctx = TestCtxBuilder::new(test_name)
            .with_anvil_http()
            .with_dataset_manifest("anvil_rpc")
            .build()
            .await
            .expect("failed to build test context");
        Self { ctx }
    }

    fn anvil(&self) -> &crate::testlib::fixtures::Anvil {
        self.ctx.anvil()
    }

    async fn get_job_events(&self, job_id: i64) -> reqwest::Response {
        let url = format!(
            "{}/jobs/{}/events",
            self.ctx.daemon_controller().admin_api_url(),
            job_id
        );

        reqwest::Client::new()
            .get(&url)
            .send()
            .await
            .expect("failed to send request")
    }

    async fn deploy_dataset(
        &self,
        namespace: &str,
        name: &str,
        revision: &str,
        end_block: Option<u64>,
    ) -> i64 {
        let ampctl = self.ctx.new_ampctl();
        let reference = format!("{}/{}@{}", namespace, name, revision);

        let job_id = ampctl
            .dataset_deploy(&reference, end_block, None, None, false)
            .await
            .expect("failed to deploy dataset");

        *job_id
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct JobEventsResponse {
    job_id: i64,
    events: Vec<JobEventInfo>,
}

#[derive(Debug, Deserialize, Serialize)]
struct JobEventInfo {
    id: i64,
    created_at: String,
    node_id: String,
    event_type: String,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error_code: String,
}
