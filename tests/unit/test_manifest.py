import pytest
import yaml

from ekscdk.constructs._manifest import load_with_subs, manifest_dir, parse_kafka_nlb_ports


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


def test_parse_kafka_nlb_ports_normal():
    ports = parse_kafka_nlb_ports(manifest_dir("kafka"))
    # Bootstrap 1件 + brokers 3件 = 4件
    assert len(ports) == 4

    name, listener_port, node_port = ports[0]
    assert name == "Bootstrap"
    assert listener_port == 9094
    assert node_port == 30094

    for i, (name, listener_port, node_port) in enumerate(ports[1:]):
        assert name == f"Broker{i}"
        assert node_port == 30095 + i
        assert listener_port == 9095 + i


def test_parse_kafka_nlb_ports_missing_external_listener(tmp_path):
    manifest = {"spec": {"kafka": {"listeners": [{"name": "plain", "port": 9092, "type": "internal"}]}}}
    (tmp_path / "kafka-cluster.yaml").write_text(yaml.dump(manifest))
    with pytest.raises(ValueError, match="external"):
        parse_kafka_nlb_ports(str(tmp_path))


def test_parse_kafka_nlb_ports_missing_configuration(tmp_path):
    manifest = {"spec": {"kafka": {"listeners": [{"name": "external", "port": 9094, "type": "nodeport"}]}}}
    (tmp_path / "kafka-cluster.yaml").write_text(yaml.dump(manifest))
    with pytest.raises(ValueError, match="configuration"):
        parse_kafka_nlb_ports(str(tmp_path))


def test_parse_kafka_nlb_ports_missing_spec_path(tmp_path):
    manifest = {"spec": {}}  # kafka キーが欠落
    (tmp_path / "kafka-cluster.yaml").write_text(yaml.dump(manifest))
    with pytest.raises(ValueError, match="spec.kafka.listeners"):
        parse_kafka_nlb_ports(str(tmp_path))


def test_parse_kafka_nlb_ports_missing_bootstrap_nodeport(tmp_path):
    manifest = {
        "spec": {"kafka": {"listeners": [{
            "name": "external", "port": 9094, "type": "nodeport",
            "configuration": {"bootstrap": {}, "brokers": []},  # nodePort 欠落
        }]}}
    }
    (tmp_path / "kafka-cluster.yaml").write_text(yaml.dump(manifest))
    with pytest.raises(ValueError, match="bootstrap.*nodePort|nodePort.*bootstrap"):
        parse_kafka_nlb_ports(str(tmp_path))


def test_parse_kafka_nlb_ports_broker_missing_field(tmp_path):
    manifest = {
        "spec": {"kafka": {"listeners": [{
            "name": "external", "port": 9094, "type": "nodeport",
            "configuration": {
                "bootstrap": {"nodePort": 30094},
                "brokers": [{"broker": 0, "nodePort": 30095}],  # advertisedPort 欠落
            },
        }]}}
    }
    (tmp_path / "kafka-cluster.yaml").write_text(yaml.dump(manifest))
    with pytest.raises(ValueError, match="advertisedPort"):
        parse_kafka_nlb_ports(str(tmp_path))
