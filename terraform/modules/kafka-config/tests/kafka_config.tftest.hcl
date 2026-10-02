variables {
  manifest_dir    = "../../../cdk/manifests/kafka"
  advertised_host = "my-nlb.example.com"
  broker_count    = 3
}

run "external_listener_is_read_from_manifest" {
  command = plan

  assert {
    condition     = output.external_listener_name == "external"
    error_message = "external listener 名が kafka-cluster.yaml から取得できていない"
  }

  assert {
    condition     = output.external_listener_port == 9094
    error_message = "external listener の port が kafka-cluster.yaml から取得できていない"
  }
}

run "default_broker_count_matches_legacy_layout" {
  command = plan

  assert {
    condition = output.nlb_ports == [
      { name = "Bootstrap", listener_port = 9094, node_port = 30094 },
      { name = "Broker0", listener_port = 9095, node_port = 30095 },
      { name = "Broker1", listener_port = 9096, node_port = 30096 },
      { name = "Broker2", listener_port = 9097, node_port = 30097 },
    ]
    error_message = "broker_count=3 の NLB port が従来の採番と一致しない"
  }
}

run "nlb_ports_scale_with_broker_count" {
  command = plan

  variables {
    broker_count = 5
  }

  assert {
    condition     = length(output.nlb_ports) == 6
    error_message = "broker_count=5 で Bootstrap + broker 5 件にならない"
  }

  assert {
    condition     = output.nlb_ports[5] == { name = "Broker4", listener_port = 9099, node_port = 30099 }
    error_message = "末尾 broker の採番が既存 broker の続きになっていない"
  }
}

run "zero_broker_count_returns_bootstrap_only" {
  command = plan

  variables {
    broker_count = 0
  }

  assert {
    condition     = output.nlb_ports == [{ name = "Bootstrap", listener_port = 9094, node_port = 30094 }]
    error_message = "broker_count=0 で Bootstrap のみにならない"
  }

  assert {
    condition     = length(output.broker_configs) == 0
    error_message = "broker_count=0 で broker 設定が空にならない"
  }
}

run "broker_configs_match_legacy_layout" {
  command = plan

  assert {
    condition = output.broker_configs == [
      { broker = 0, advertisedHost = "my-nlb.example.com", nodePort = 30095, advertisedPort = 9095 },
      { broker = 1, advertisedHost = "my-nlb.example.com", nodePort = 30096, advertisedPort = 9096 },
      { broker = 2, advertisedHost = "my-nlb.example.com", nodePort = 30097, advertisedPort = 9097 },
    ]
    error_message = "broker_count=3 の broker 設定が従来の brokers[] と一致しない"
  }
}

run "negative_broker_count_is_rejected" {
  command = plan

  variables {
    broker_count = -1
  }

  expect_failures = [var.broker_count]
}

run "missing_spec_path_is_rejected" {
  command = plan

  variables {
    manifest_dir = "tests/fixtures/no-listeners"
  }

  expect_failures = [output.external_listener_name]
}

run "missing_external_listener_is_rejected" {
  command = plan

  variables {
    manifest_dir = "tests/fixtures/no-external"
  }

  expect_failures = [output.external_listener_name]
}

run "missing_configuration_is_rejected" {
  command = plan

  variables {
    manifest_dir = "tests/fixtures/no-configuration"
  }

  expect_failures = [output.nlb_ports]
}

run "missing_bootstrap_nodeport_is_rejected" {
  command = plan

  variables {
    manifest_dir = "tests/fixtures/no-bootstrap-nodeport"
  }

  expect_failures = [output.nlb_ports]
}
