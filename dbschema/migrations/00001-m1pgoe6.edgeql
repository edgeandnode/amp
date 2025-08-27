CREATE MIGRATION m1pgoe6rksu4jmjeharwhwkin5tlpe3ckjylkaajt2phvc6jekbvcq
    ONTO initial
{
  CREATE EXTENSION pgvector VERSION '0.7';
  CREATE EXTENSION pg_trgm VERSION '1.6';
  CREATE EXTENSION ai VERSION '1.0';
  CREATE SCALAR TYPE default::DatasetMutability EXTENDING enum<Mutable, Snapshot, AppendOnly>;
  CREATE SCALAR TYPE default::DatasetStatus EXTENDING enum<Draft, Published, Deprecated, Archived>;
  CREATE SCALAR TYPE default::DatasetVisibility EXTENDING enum<Private, Public>;
  CREATE FUTURE simple_scoping;
  CREATE ABSTRACT TYPE default::DatasetDiscovery {
      CREATE PROPERTY description: std::str {
          CREATE CONSTRAINT std::max_len_value(1024);
      };
      CREATE REQUIRED PROPERTY indexing_chains: array<std::str>;
      CREATE PROPERTY keywords: array<std::str>;
      CREATE REQUIRED PROPERTY source: array<std::str>;
      CREATE REQUIRED PROPERTY visibility: default::DatasetVisibility {
          SET default := (default::DatasetVisibility.Public);
      };
      CREATE INDEX ON (.indexing_chains);
  };
  CREATE ABSTRACT TYPE default::DatasetMetadata {
      CREATE REQUIRED PROPERTY owner: std::str;
      CREATE INDEX ON (.owner);
      CREATE PROPERTY license: std::str;
      CREATE PROPERTY mutability: default::DatasetMutability;
      CREATE PROPERTY repository_url: std::str {
          CREATE CONSTRAINT std::regexp(r'^https?://[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*(/.*)?$');
      };
      CREATE PROPERTY runtime_config: std::json;
  };
  CREATE TYPE default::Dataset EXTENDING default::DatasetDiscovery, default::DatasetMetadata {
      CREATE REQUIRED PROPERTY name: std::str {
          CREATE CONSTRAINT std::regexp('^[a-z][a-z0-9_-]*$');
      };
      CREATE REQUIRED PROPERTY namespace: std::str;
      CREATE CONSTRAINT std::exclusive ON ((.namespace, .name));
      CREATE INDEX ON (std::str_lower(((.namespace ++ '/') ++ .name)));
      CREATE INDEX std::fts::index ON ((std::fts::with_options(std::array_join(.keywords, ' '), language := std::fts::Language.eng, weight_category := std::fts::Weight.A), std::fts::with_options(std::array_join(.source, ' '), language := std::fts::Language.eng, weight_category := std::fts::Weight.B), std::fts::with_options(std::array_join(.indexing_chains, ' '), language := std::fts::Language.eng, weight_category := std::fts::Weight.C), std::fts::with_options(.description, language := std::fts::Language.eng, weight_category := std::fts::Weight.D)));
      CREATE INDEX std::pg::btree ON (std::str_lower(((.namespace ++ '/') ++ .name)));
      CREATE REQUIRED PROPERTY created_at: std::datetime {
          SET default := (std::datetime_of_statement());
      };
      CREATE REQUIRED PROPERTY dataset_id: std::str {
          CREATE CONSTRAINT std::exclusive;
      };
      CREATE PROPERTY dataset_name := (std::str_lower(((.namespace ++ '/') ++ .name)));
      CREATE REQUIRED PROPERTY status: default::DatasetStatus {
          SET default := (default::DatasetStatus.Draft);
      };
  };
  ALTER TYPE default::DatasetMetadata {
      CREATE MULTI LINK ancestors: default::Dataset {
          CREATE CONSTRAINT std::exclusive;
      };
  };
  ALTER TYPE default::Dataset {
      CREATE LINK descendants := (.<ancestors[IS default::Dataset]);
  };
  CREATE TYPE default::DatasetVersion {
      CREATE REQUIRED PROPERTY created_at: std::datetime {
          SET default := (std::datetime_of_statement());
      };
      CREATE REQUIRED PROPERTY status: default::DatasetStatus {
          SET default := (default::DatasetStatus.Draft);
      };
      CREATE REQUIRED PROPERTY label: std::str;
      CREATE INDEX ON (.label);
      CREATE REQUIRED PROPERTY dataset_version_id: std::str;
      CREATE INDEX ON (.dataset_version_id);
      CREATE PROPERTY breaking: std::bool {
          SET default := false;
      };
      CREATE PROPERTY changelog: std::str;
      CREATE REQUIRED PROPERTY manifest: std::str;
  };
  ALTER TYPE default::Dataset {
      CREATE MULTI LINK versions: default::DatasetVersion {
          ON SOURCE DELETE DELETE TARGET IF ORPHAN;
          CREATE CONSTRAINT std::exclusive;
      };
      CREATE LINK latest_version := (SELECT
          .versions FILTER
              (.status = default::DatasetStatus.Published)
          ORDER BY
              .created_at DESC
      LIMIT
          1
      );
  };
  ALTER TYPE default::DatasetVersion {
      CREATE SINGLE LINK dataset := (.<versions[IS default::Dataset]);
  };
};
