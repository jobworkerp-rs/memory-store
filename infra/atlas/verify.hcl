env "sqlite" {
  url = getenv("MEMORIES_ATLAS_DATABASE_URL")
  dev = "sqlite://memories_atlas_verify?mode=memory&_fk=1"
  migration {
    dir = "file://sqlite/migrations"
  }
}

env "postgres" {
  url = getenv("MEMORIES_ATLAS_DATABASE_URL")
  dev = getenv("MEMORIES_ATLAS_INTERNAL_DEV_URL")
  migration {
    dir = "file://postgres/migrations"
  }
}
