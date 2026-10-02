locals {
  # 既存 broker の値を変えるとクライアント接続が壊れるため変更しない。
  # broker を増やすときは末尾に追加採番される。
  broker_node_port_start     = 30095
  broker_listener_port_start = 9095

  manifest  = yamldecode(file("${var.manifest_dir}/kafka-cluster.yaml"))
  listeners = try(local.manifest.spec.kafka.listeners, null)
  external  = try([for l in local.listeners : l if l.name == "external"][0], null)

  external_port       = try(local.external.port, null)
  configuration       = try(local.external.configuration, null)
  bootstrap           = try(local.configuration.bootstrap, null)
  bootstrap_node_port = try(local.bootstrap.nodePort, null)

  nlb_ports = concat(
    [{ name = "Bootstrap", listener_port = local.external_port, node_port = local.bootstrap_node_port }],
    [
      for i in range(var.broker_count) : {
        name          = "Broker${i}"
        listener_port = local.broker_listener_port_start + i
        node_port     = local.broker_node_port_start + i
      }
    ],
  )

  broker_configs = [
    for i in range(var.broker_count) : {
      broker         = i
      advertisedHost = var.advertised_host
      nodePort       = local.broker_node_port_start + i
      advertisedPort = local.broker_listener_port_start + i
    }
  ]
}
