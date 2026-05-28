from kinopio_hub_ros.core.sync_policy import LatestStatePolicy


def test_throttle_emits_only_latest_value_in_window():
    policy = LatestStatePolicy(throttle_ms=100, dedupe=True, loop_suppression_ms=1000)

    assert (
        policy.ingest_ros_text(
            "/chatter",
            "one",
            now_ms=0,
            message_type="std_msgs/msg/String",
        )
        == ()
    )
    assert (
        policy.ingest_ros_text(
            "/chatter",
            "two",
            now_ms=25,
            message_type="std_msgs/msg/String",
        )
        == ()
    )
    assert (
        policy.ingest_ros_text(
            "/chatter",
            "three",
            now_ms=50,
            message_type="std_msgs/msg/String",
        )
        == ()
    )

    emissions = policy.flush_due(now_ms=100)

    assert len(emissions) == 1
    assert emissions[0].topic == "/chatter"
    assert emissions[0].text == "three"
    assert emissions[0].message_type == "std_msgs/msg/String"
    assert emissions[0].first_observed_at_ms == 0
    assert emissions[0].last_observed_at_ms == 50
    assert emissions[0].emitted_at_ms == 100


def test_dedupe_skips_republishing_same_text():
    policy = LatestStatePolicy(throttle_ms=0, dedupe=True, loop_suppression_ms=1000)

    first = policy.ingest_ros_text(
        "/chatter",
        "same",
        now_ms=0,
        message_type="std_msgs/msg/String",
    )
    second = policy.ingest_ros_text(
        "/chatter",
        "same",
        now_ms=1,
        message_type="std_msgs/msg/String",
    )

    assert [emission.text for emission in first] == ["same"]
    assert second == ()
    assert policy.latest_published_text("/chatter") == "same"


def test_loop_suppression_blocks_recent_writeback_echo():
    policy = LatestStatePolicy(throttle_ms=0, dedupe=True, loop_suppression_ms=1000)
    policy.record_nats_writeback(
        "/chatter",
        "echo",
        now_ms=10,
        message_type="std_msgs/msg/String",
    )

    assert policy.ingest_ros_text(
        "/chatter",
        "echo",
        now_ms=20,
        message_type="std_msgs/msg/String",
    ) == ()
    assert policy.ingest_ros_text(
        "/chatter",
        "echo",
        now_ms=1200,
        message_type="std_msgs/msg/String",
    ) != ()


def test_latest_state_and_pending_text_are_tracked():
    policy = LatestStatePolicy(throttle_ms=200, dedupe=True, loop_suppression_ms=1000)

    policy.ingest_ros_text(
        "/robot/status/text",
        "warming",
        now_ms=100,
        message_type="std_msgs/msg/String",
    )
    policy.ingest_ros_text(
        "/robot/status/text",
        "ready",
        now_ms=150,
        message_type="std_msgs/msg/String",
    )

    assert policy.latest_seen_text("/robot/status/text") == "ready"
    assert policy.pending_text("/robot/status/text") == "ready"

    emissions = policy.flush_due(now_ms=300)

    assert [emission.text for emission in emissions] == ["ready"]
    assert policy.latest_published_text("/robot/status/text") == "ready"
    assert policy.pending_text("/robot/status/text") is None


def test_failed_emission_can_be_requeued_for_retry():
    policy = LatestStatePolicy(throttle_ms=0, dedupe=True, loop_suppression_ms=1000)
    emissions = policy.ingest_ros_text(
        "/chatter",
        "retry me",
        now_ms=100,
        message_type="std_msgs/msg/String",
    )

    policy.requeue_emissions(emissions, retry_at_ms=200)

    assert policy.latest_published_text("/chatter") is None
    assert policy.pending_text("/chatter") == "retry me"
    assert [emission.text for emission in policy.flush_due(now_ms=200)] == ["retry me"]
