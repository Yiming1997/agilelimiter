// test/ is a separate module so the harness and its scenario code never
// become a dependency of the library module. The replace pins every
// agilelimiter import to the working tree: building the harness always
// compiles the local library, never a published copy.
module agilelimiter/test

go 1.23

require agilelimiter v0.0.0

replace agilelimiter => ../
