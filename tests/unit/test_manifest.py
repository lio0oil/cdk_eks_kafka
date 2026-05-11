import pytest
import yaml

from ekscdk.constructs._manifest import (
    build_kafka_broker_configs,
    build_kafka_nlb_ports,
    load_with_subs,
    manifest_dir,
)


def test_load_with_subs_replaces_placeholders(tmp_path):
    (tmp_path / "test.yaml").write_text("host: <HOST>\nport: <PORT>")
    result = load_with_subs(str(tmp_path), "test.yaml", HOST="example.com", PORT="9094")
    assert result["host"] == "example.com"
    assert result["port"] == 9094


def test_load_with_subs_no_substitutions(tmp_path):
    (tmp_path / "test.yaml").write_text("key: value")
    result = load_with_subs(str(tmp_path), "test.yaml")
    assert result["key"] == "value"


def test_load_with_subs_unreplaced_placeholder_stays(tmp_path):
    (tmp_path / "test.yaml").write_text("host: <UNREPLACED>")
    result = load_with_subs(str(tmp_path), "test.yaml")
    assert result["host"] == "<UNREPLACED>"


def test_build_kafka_nlb_ports_default_broker_count_matches_legacy_layout():
    """broker_count=3 で従来の YAML brokers[] と同一の (name, listener_port, node_port) を得る。"""
    ports = build_kafka_nlb_ports(manifest_dir("kafka"), broker_count=3)
    assert ports == [
        ("Bootstrap", 9094, 30094),
        ("Broker0", 9095, 30095),
        ("Broker1", 9096, 30096),
        ("Broker2", 9097, 30097),
    ]


def test_build_kafka_nlb_ports_scales_with_broker_count():
    ports = build_kafka_nlb_ports(manifest_dir("kafka"), broker_count=5)
    assert len(ports) == 6
    assert ports[0] == ("Bootstrap", 9094, 30094)
    assert ports[-1] == ("Broker4", 9099, 30099)


def test_build_kafka_nlb_ports_zero_broker_count_returns_bootstrap_only():
    ports = build_kafka_nlb_ports(manifest_dir("kafka"), broker_count=0)
    assert ports == [("Bootstrap", 9094, 30094)]


def test_build_kafka_nlb_ports_missing_external_listener(tmp_path):
    manifest = {"spec": {"kafka": {"listeners": [{"name": "plain", "port": 9092, "type": "internal"}]}}}
    (tmp_path / "kafka-cluster.yaml").write_text(yaml.dump(manifest))
    with pytest.raises(ValueError, match="external"):
        build_kafka_nlb_ports(str(tmp_path), broker_count=3)


def test_build_kafka_nlb_ports_missing_configuration(tmp_path):
    manifest = {"spec": {"kafka": {"listeners": [{"name": "external", "port": 9094, "type": "nodeport"}]}}}
    (tmp_path / "kafka-cluster.yaml").write_text(yaml.dump(manifest))
    with pytest.raises(ValueError, match="configuration"):
        build_kafka_nlb_ports(str(tmp_path), broker_count=3)


def test_build_kafka_nlb_ports_missing_spec_path(tmp_path):
    manifest = {"spec": {}}
    (tmp_path / "kafka-cluster.yaml").write_text(yaml.dump(manifest))
    with pytest.raises(ValueError, match="spec.kafka.listeners"):
        build_kafka_nlb_ports(str(tmp_path), broker_count=3)


def test_build_kafka_nlb_ports_missing_bootstrap_nodeport(tmp_path):
    manifest = {
        "spec": {
            "kafka": {
                "listeners": [
                    {
                        "name": "external",
                        "port": 9094,
                        "type": "nodeport",
                        "configuration": {
                            "bootstrap": {},
                        },
                    }
                ]
            }
        }
    }
    (tmp_path / "kafka-cluster.yaml").write_text(yaml.dump(manifest))
    with pytest.raises(ValueError, match="bootstrap.*nodePort|nodePort.*bootstrap"):
        build_kafka_nlb_ports(str(tmp_path), broker_count=3)


def test_build_kafka_broker_configs_default_layout_matches_legacy():
    """broker_count=3 で従来 YAML の brokers[] と同一構造の dict 列を生成する。"""
    configs = build_kafka_broker_configs(broker_count=3, advertised_host="my-nlb.example.com")
    assert configs == [
        {"broker": 0, "advertisedHost": "my-nlb.example.com", "nodePort": 30095, "advertisedPort": 9095},
        {"broker": 1, "advertisedHost": "my-nlb.example.com", "nodePort": 30096, "advertisedPort": 9096},
        {"broker": 2, "advertisedHost": "my-nlb.example.com", "nodePort": 30097, "advertisedPort": 9097},
    ]


def test_build_kafka_broker_configs_key_order_matches_strimzi_yaml():
    """dict 挿入順が従来 YAML の (broker, advertisedHost, nodePort, advertisedPort) と一致する。

    CDK が manifest を JSON 化する際にキー順が変わると、CFN 差分・Strimzi 側の noop 判定に
    影響しうるため、従来 YAML の順序を維持することを明示的に検証する。
    """
    configs = build_kafka_broker_configs(broker_count=1, advertised_host="h")
    assert list(configs[0].keys()) == ["broker", "advertisedHost", "nodePort", "advertisedPort"]


def test_build_kafka_broker_configs_zero_returns_empty():
    assert build_kafka_broker_configs(broker_count=0, advertised_host="h") == []
