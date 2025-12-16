group "default" {
  targets = ["postgresql", "mysql"]
}

variable "TAG" {
  default = "latest"
}

variable "GO_VERSION" {
  default = "1.24"
}

variable "DEBIAN_RELEASE" {
  default = "bookworm"
}

variable "MYSQL_VERSION" {
  default = "8.0"
}

variable "IS_RELEASE" {
  default = false
}

target "postgresql" {
  context = "."
  dockerfile = "docker/greenmask/Dockerfile"

  platforms = [
    "linux/amd64",
    "linux/arm64",
  ]

  args = {
    DEBIAN_RELEASE = "${DEBIAN_RELEASE}",
    GO_VERSION = "${GO_VERSION}"
  }

  tags = compact([
    "greenmask/greenmask:${TAG}",
    "ghcr.io/greenmaskio/greenmask:${TAG}",
    IS_RELEASE ? "greenmask/greenmask:latest" : null,
    IS_RELEASE ? "ghcr.io/greenmaskio/greenmask:latest" : null,
  ])
}

target "mysql" {
  context = "./v1"
  dockerfile = "docker/greenmask/mysql/main/Dockerfile"

  platforms = [
    "linux/amd64",
  ]

  args = {
    MYSQL_VERSION = "${MYSQL_VERSION}",
    DEBIAN_RELEASE = "${DEBIAN_RELEASE}",
    GO_VERSION = "${GO_VERSION}"
  }

  tags = compact([
    "greenmask:mysql-${MYSQL_VERSION}-${TAG}",
    "ghcr.io/greenmaskio/greenmask:mysql-${MYSQL_VERSION}-${TAG}"
  ])
}