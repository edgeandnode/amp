# pgpq submodule

This submodule is a direct port of [this pgpq github repo](https://github.com/adriangb/pgpq).

The reason is this github package is seemingly unmaintained and the arrow versions it is using is pretty out of date,
and multiple major versions behind the arrow-{schema,array} versions we are using in the nozzle crate; which was causing
compilation failures.