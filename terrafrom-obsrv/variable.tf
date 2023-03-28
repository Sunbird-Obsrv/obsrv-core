variable "releases" {
  type = list(object({
    name = string
  }))
  default = [
    {
      name = "merged-pipeline"
    },
    {
      name = "preprocessor"
    },
    {
      name = "transformer"
    },
    {
      name = "extractor"
    },
    {
      name = "druid-router"
    },
    {
      name = "denormalizer"
    }
  ]
}

variable "helm_release_to_deploy" {
  type = string
  default = ""
}



