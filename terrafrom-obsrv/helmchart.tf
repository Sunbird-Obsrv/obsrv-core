# Define the provider
 provider "helm" {
  kubernetes {
    config_path = "/home/divya/Downloads/kindkube.yaml"
  }
}


locals {
  release_map = { for r in var.releases : r.name => r }
}

resource "helm_release" "example" {
  for_each = var.helm_release_to_deploy != "" ? { for k, v in local.release_map : k => v if k == var.helm_release_to_deploy } : local.release_map

  name       = each.value.name
  repository = "./test"
  chart      = "flink"
  version    = "1.0.0"
  namespace  = "flink-local"

  values = [
    file("terrafrom-obsrv/test/flink/values.yaml")
  ]
}

