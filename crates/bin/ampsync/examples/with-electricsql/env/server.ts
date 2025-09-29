import { createEnv } from "@t3-oss/env-nextjs";
import { Schema } from "effect";

export const env = createEnv({
  server: {
    ELECTRIC_URL: Schema.standardSchemaV1(Schema.URL),
    NODE_ENV: Schema.standardSchemaV1(
      Schema.Literal("development", "test", "production")
    ),
  },
  runtimeEnv: {
    ELECTRIC_URL: process.env.ELECTRIC_URL || "http://localhost:3000",
    NODE_ENV: process.env.NODE_ENV || "development",
  },
});
