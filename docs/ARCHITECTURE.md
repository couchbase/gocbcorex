# Architecture

### Contexts

gocbcorex makes extensive use of context.Context through it's code. All functions
which can block _must_ take a context.Context, and conversely it can be assumed that
any function which does not accept a context.Context will never block.
