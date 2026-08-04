env "sqlite" {
  url = getenv("MEMORIES_ATLAS_DATABASE_URL")
  migration {
    dir = "file://sqlite/migrations"
  }
}

env "postgres" {
  url = getenv("MEMORIES_ATLAS_DATABASE_URL")
  migration {
    dir = "file://postgres/migrations"
  }
}
