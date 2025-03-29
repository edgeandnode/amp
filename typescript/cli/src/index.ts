import { Schema } from "effect";
import * as Model from "./Model.js";

export type Context = any;

export function defineDataset(
  fn: (ctx: Context) => Model.DatasetDefinition,
): Model.DatasetDefinition {
  return Schema.decodeUnknownSync(Model.DatasetDefinition)(fn({}));
}
