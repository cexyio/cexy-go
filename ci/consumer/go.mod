// A separate module that uses cexy-go the way a user's program does: only the SDK's own
// dependencies, none of its test-only code. See CexyQA's consumer-build rule (2026-09-28).
module example.com/cexy-consumer-check

go 1.26.6

require github.com/cexyio/cexy-go v0.0.0

require github.com/coder/websocket v1.8.15 // indirect

replace github.com/cexyio/cexy-go => ../..
