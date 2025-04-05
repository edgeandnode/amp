import { Schema } from "effect";
import * as Model from "./Model.js";

export * as Model from "./Model.js";
export * as Api from "./Api.js";
export * as ManifestBuilder from "./ManifestBuilder.js";
export * as ManifestDeployer from "./ManifestDeployer.js";

export const defineDataset = (fn: () => Model.DatasetDefinition) => {
  return Schema.decodeUnknownSync(Model.DatasetDefinition)(fn());
}
