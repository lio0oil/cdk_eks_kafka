import re

import pytest
import yaml

from ekscdk.constructs._manifest import (
    build_kafka_broker_configs,
    build_kafka_nlb_ports,
    load_manifest,
    load_manifest_with_subs,
    manifest_dir,
)


def test_load_manifest_with_subs_replaces_placeholders(tmp_path):
    (tmp_path / "test.yaml").write_text("host: <HOST>\nport: <PORT>")
    result = load_manifest_with_subs(str(tmp_path), "test.yaml", HOST="example.com", PORT="9094")
    assert result["host"] == "example.com"
    assert result["port"] == 9094


def test_load_manifest_with_subs_no_substitutions(tmp_path):
    (tmp_path / "test.yaml").write_text("key: value")
    result = load_manifest_with_subs(str(tmp_path), "test.yaml")
    assert result["key"] == "value"


def test_load_manifest_with_subs_unreplaced_placeholder_stays(tmp_path):
    (tmp_path / "test.yaml").write_text("host: <UNREPLACED>")
    result = load_manifest_with_subs(str(tmp_path), "test.yaml")
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


def _kafka_rules_by_alert() -> dict[str, str]:
    manifest = load_manifest(
        manifest_dir("monitoring"), "prometheus-install/prometheus-rules/prometheus-rules-kafka.yaml"
    )
    return {r["alert"]: r["expr"] for r in manifest["spec"]["groups"][0]["rules"]}


def test_kafka_rules_pod_regex_matches_actual_broker_and_controller_pod_names():
    """prometheus-rules-kafka.yaml の Pod/PVC 名 regex が実際の NodePool 命名（broker/controller）に一致する。

    Strimzi 公式サンプルは NodePool 名が `kafka` である前提の regex（`.+-kafka-[0-9]+`）を
    使うが、本プロジェクトは broker/controller という別名にしているため、無編集のままだと
    ScrapeProblem / KafkaContainerRestartedInTheLast5Minutes / KafkaRunningOutOfSpace が
    実際の Pod に一致せず機能しない（up 系・PVC 系は一致 series が無く常時未発火になる）。
    これらは broker/controller どちらの障害でも意味が壊れないため両方にマッチしてよい
    （KafkaBrokerContainersDown は absent() の意味が変わるため対象外、別テストで検証）。
    """
    rules_by_alert = _kafka_rules_by_alert()

    sample_pod_names = ["kafka-cluster-broker-0", "kafka-cluster-controller-0"]
    for alert_name, label_pattern in [
        ("ScrapeProblem", r'kubernetes_pod_name=~"([^"]+)"'),
        ("KafkaContainerRestartedInTheLast5Minutes", r'pod=~"([^"]+)"'),
    ]:
        expr = rules_by_alert[alert_name]
        match = re.search(label_pattern, expr)
        assert match, f"{alert_name} に pod 名 regex が見つからない: {expr!r}"
        pod_regex = match.group(1)
        for pod_name in sample_pod_names:
            assert re.fullmatch(pod_regex, pod_name), (
                f"{alert_name} の regex {pod_regex!r} が実際の Pod 名 {pod_name!r} にマッチしない"
            )

    pvc_match = re.search(r'persistentvolumeclaim=~"([^"]+)"', rules_by_alert["KafkaRunningOutOfSpace"])
    assert pvc_match
    pvc_regex = pvc_match.group(1)
    for pvc_name in ["data-0-kafka-cluster-broker-0", "data-0-kafka-cluster-controller-0"]:
        assert re.fullmatch(pvc_regex, pvc_name), (
            f"KafkaRunningOutOfSpace の regex {pvc_regex!r} が実際の PVC 名 {pvc_name!r} にマッチしない"
        )


def test_kafka_broker_containers_down_matches_broker_only_not_controller():
    """KafkaBrokerContainersDown は absent() ベースのため、broker/controller 両方を

    regex に含めると「両方同時に全滅した時だけ」発火する条件に意味が変わってしまう
    （どちらか一方のみ全滅した場合に検知できなくなる）。アラート名の通り broker の
    container 消失だけを対象にする regex であることを固定する。
    """
    expr = _kafka_rules_by_alert()["KafkaBrokerContainersDown"]
    match = re.search(r'pod=~"([^"]+)"', expr)
    assert match, f"KafkaBrokerContainersDown に pod 名 regex が見つからない: {expr!r}"
    pod_regex = match.group(1)
    assert re.fullmatch(pod_regex, "kafka-cluster-broker-0")
    assert not re.fullmatch(pod_regex, "kafka-cluster-controller-0")
